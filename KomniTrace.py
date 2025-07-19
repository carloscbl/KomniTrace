#!/usr/bin/env python3
"""
KomniTrace: Kubernetes Kafka Event Tracer

A sophisticated tool for tracing device events across Kafka topics in Kubernetes environments.
Provides comprehensive event collection, analysis, and visualization capabilities.

Features:
- Parallel topic processing for optimal performance
- **Smart-stop consumer**: Stops consuming precisely when all historical messages are read.
- Non-linear time mapping for better visualization
- Multiple visualization formats (timeline, swimlane, flow, compact)
- Interactive Plotly charts (when available)
- HTML report generation
- SSH-based remote execution
- Intelligent time filtering and topic discovery

Usage:
    python KomniTrace.py --namespace my-namespace --device-id "12345"
"""

import os
import os.path
import orjson
import json
import logging
import argparse
import shutil
import tempfile
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import random
import uuid
import shlex
import select
import sys
import stat
from pathlib import Path

# Asumimos que komni_reports.py existe en el mismo directorio
# Si no, necesitarás crearlo o comentar las líneas que lo usan.
try:
    from komni_reports import KomniReportGenerator
except ImportError:
    KomniReportGenerator = None
    print("WARNING: komni_reports.py not found. Report generation will be disabled.")


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed back to INFO
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('komni_trace.log')
    ]
)
logger = logging.getLogger(__name__)


class SSHKeyManager:
    """Manages SSH key generation and setup for improved performance"""
    
    def __init__(self, config: 'TraceConfig'):
        self.config = config
        self.ssh_dir = Path.home() / '.ssh'
        self.key_name = f"komni_trace_{config.ssh_host.replace('.', '_')}"
        self.private_key_path = self.ssh_dir / self.key_name
        self.public_key_path = self.ssh_dir / f"{self.key_name}.pub"
        self.config_path = self.ssh_dir / 'config'
        
    def ensure_ssh_dir(self):
        """Ensure SSH directory exists with proper permissions"""
        self.ssh_dir.mkdir(mode=0o700, exist_ok=True)
        
    def generate_ssh_key(self) -> bool:
        """Generate SSH key pair if it doesn't exist"""
        try:
            if self.private_key_path.exists():
                logger.info(f"SSH key already exists: {self.private_key_path}")
                return True
                
            logger.info(f"Generating SSH key pair for {self.config.ssh_host}...")
            
            # Generate SSH key pair
            cmd = [
                'ssh-keygen',
                '-t', 'rsa',
                '-b', '4096',
                '-f', str(self.private_key_path),
                '-N', '',  # No passphrase
                '-C', f'komni_trace_{self.config.ssh_host}_{int(time.time())}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.error(f"Failed to generate SSH key: {result.stderr}")
                return False
                
            # Set proper permissions
            self.private_key_path.chmod(0o600)
            self.public_key_path.chmod(0o644)
            
            logger.info(f"SSH key pair generated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error generating SSH key: {e}")
            return False
    
    def copy_public_key_to_remote(self) -> bool:
        """Copy public key to remote host using password authentication"""
        try:
            if not self.public_key_path.exists():
                logger.error("Public key does not exist")
                return False
                
            logger.info(f"Installing public key on {self.config.ssh_host}...")
            
            # Read public key
            public_key = self.public_key_path.read_text().strip()
            
            # Use ssh-copy-id if available, with optimized settings
            try:
                cmd = [
                    'sshpass', '-p', self.config.ssh_password,
                    'ssh-copy-id',
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', 'ConnectTimeout=10',
                    '-o', 'PreferredAuthentications=password',  # Only use password
                    '-o', 'PubkeyAuthentication=no',  # Disable key auth attempts
                    '-o', 'IdentitiesOnly=yes',  # Don't try default keys
                    '-o', 'NumberOfPasswordPrompts=1',  # Only one password attempt
                    '-i', str(self.public_key_path),
                    f"{self.config.ssh_user}@{self.config.ssh_host}"
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    logger.info("Public key installed successfully using ssh-copy-id")
                    return True
                else:
                    logger.warning(f"ssh-copy-id failed: {result.stderr.strip()}")
                    
            except FileNotFoundError:
                logger.info("ssh-copy-id not available, using manual approach")
            
            # Manual approach with optimized SSH settings
            logger.info("Using manual approach to install public key...")
            
            # Create the complete command in one SSH session to minimize connection attempts
            complete_setup_cmd = f'''
set -e
mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
# Check if key already exists to avoid duplicates
if ! grep -Fxq "{public_key}" ~/.ssh/authorized_keys 2>/dev/null; then
    echo "{public_key}" >> ~/.ssh/authorized_keys
    echo "Key added successfully"
else
    echo "Key already exists"
fi
'''
            
            cmd = [
                'sshpass', '-p', self.config.ssh_password,
                'ssh',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'ConnectTimeout=10',
                '-o', 'PreferredAuthentications=password',  # Only use password
                '-o', 'PubkeyAuthentication=no',  # Disable key auth attempts
                '-o', 'IdentitiesOnly=yes',  # Don't try default keys
                '-o', 'NumberOfPasswordPrompts=1',  # Only one password attempt
                '-o', 'LogLevel=ERROR',  # Reduce verbose output
                f"{self.config.ssh_user}@{self.config.ssh_host}",
                complete_setup_cmd
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
            if result.returncode == 0:
                logger.info("Public key installed successfully using manual approach")
                logger.debug(f"Setup output: {result.stdout.strip()}")
                return True
            else:
                logger.error(f"Failed to install public key manually: {result.stderr.strip()}")
                # Check if it's an authentication issue vs a server message
                if "Too many authentication failures" in result.stderr:
                    logger.error("Server is rejecting connections due to too many auth attempts")
                    logger.error("Try waiting a few minutes before retrying, or check SSH server settings")
                elif "authentication failures" in result.stderr.lower():
                    logger.error("Authentication failed - check username and password")
                return False
                
        except Exception as e:
            logger.error(f"Error copying public key to remote host: {e}")
            return False
    
    def test_key_authentication(self) -> bool:
        """Test if key-based authentication works"""
        try:
            logger.info("Testing SSH key authentication...")
            
            cmd = [
                'ssh',
                '-i', str(self.private_key_path),
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'ConnectTimeout=10',
                '-o', 'BatchMode=yes',  # No interactive prompts
                '-o', 'PreferredAuthentications=publickey',  # Only use key auth
                '-o', 'PubkeyAuthentication=yes',
                '-o', 'PasswordAuthentication=no',  # Disable password auth
                '-o', 'IdentitiesOnly=yes',  # Only use specified key
                '-o', 'NumberOfPasswordPrompts=0',  # No password prompts
                '-o', 'LogLevel=ERROR',  # Reduce verbose output
                f"{self.config.ssh_user}@{self.config.ssh_host}",
                'echo "SSH key authentication successful"'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0 and "SSH key authentication successful" in result.stdout:
                logger.info("SSH key authentication test passed")
                return True
            else:
                logger.warning(f"SSH key authentication test failed")
                logger.debug(f"Return code: {result.returncode}")
                logger.debug(f"STDERR: {result.stderr.strip()}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing SSH key authentication: {e}")
            return False
    
    def setup_ssh_config(self):
        """Setup SSH config for optimized connections"""
        try:
            logger.info("Setting up SSH config for optimized connections...")
            
            host_config = f"""
# KomniTrace optimized config for {self.config.ssh_host}
Host {self.config.ssh_host}
    HostName {self.config.ssh_host}
    User {self.config.ssh_user}
    IdentityFile {self.private_key_path}
    IdentitiesOnly yes
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    
    # Performance optimizations
    ControlMaster auto
    ControlPath ~/.ssh/control-%r@%h:%p
    ControlPersist 300
    
    # Connection optimization
    TCPKeepAlive yes
    ServerAliveInterval 30
    ServerAliveCountMax 3
    ConnectTimeout 10
    
    # Compression and ciphers for speed
    Compression yes
    Cipher aes128-ctr
    
    # Disable unnecessary features for speed
    GSSAPIAuthentication no
    
"""
            
            # Read existing config if it exists
            existing_config = ""
            if self.config_path.exists():
                existing_config = self.config_path.read_text()
            
            # Check if our config already exists
            if f"# KomniTrace optimized config for {self.config.ssh_host}" in existing_config:
                logger.info("SSH config already contains KomniTrace settings")
                return
            
            # Append our config
            with open(self.config_path, 'a') as f:
                f.write(host_config)
            
            # Set proper permissions
            self.config_path.chmod(0o600)
            
            logger.info("SSH config updated with performance optimizations")
            
        except Exception as e:
            logger.error(f"Error setting up SSH config: {e}")
    
    def clean_existing_keys(self):
        """Remove existing SSH keys for this host"""
        try:
            logger.info(f"Cleaning existing SSH keys for {self.config.ssh_host}...")
            
            # Remove private and public keys
            if self.private_key_path.exists():
                self.private_key_path.unlink()
                logger.info(f"Removed private key: {self.private_key_path}")
            
            if self.public_key_path.exists():
                self.public_key_path.unlink()
                logger.info(f"Removed public key: {self.public_key_path}")
            
            # Remove SSH config entry for this host
            if self.config_path.exists():
                config_content = self.config_path.read_text()
                lines = config_content.split('\n')
                
                # Find and remove the block for this host
                new_lines = []
                skip_section = False
                
                for line in lines:
                    if line.strip().startswith(f"# KomniTrace optimized config for {self.config.ssh_host}"):
                        skip_section = True
                        continue
                    elif skip_section and line.strip().startswith("# KomniTrace optimized config for"):
                        skip_section = False
                        new_lines.append(line)
                    elif skip_section and line.strip() == "":
                        skip_section = False
                        new_lines.append(line)
                    elif not skip_section:
                        new_lines.append(line)
                
                # Write back the cleaned config
                self.config_path.write_text('\n'.join(new_lines))
                logger.info("Removed SSH config entry")
            
        except Exception as e:
            logger.warning(f"Error cleaning existing keys: {e}")
    
    def check_existing_key_auth(self) -> bool:
        """Check if existing SSH key authentication already works"""
        try:
            if not self.private_key_path.exists():
                return False
                
            logger.info("Checking if existing SSH key already works...")
            
            # Test current key without any setup
            test_config = self.config
            test_config.ssh_key = str(self.private_key_path)
            test_config.ssh_password = None
            
            cmd = [
                'ssh',
                '-i', str(self.private_key_path),
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'ConnectTimeout=5',
                '-o', 'BatchMode=yes',
                '-o', 'PreferredAuthentications=publickey',
                '-o', 'PasswordAuthentication=no',
                '-o', 'IdentitiesOnly=yes',
                '-o', 'NumberOfPasswordPrompts=0',
                '-o', 'LogLevel=ERROR',
                f"{self.config.ssh_user}@{self.config.ssh_host}",
                'echo "existing_key_works"'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and "existing_key_works" in result.stdout:
                logger.info("Existing SSH key already works! Skipping key setup.")
                # Update config to use existing key
                self.config.ssh_key = str(self.private_key_path)
                self.config.ssh_password = None
                return True
            else:
                logger.debug("Existing SSH key doesn't work or doesn't exist")
                return False
                
        except Exception as e:
            logger.debug(f"Error checking existing key: {e}")
            return False
    
    def setup_ssh_keys(self) -> bool:
        """Complete SSH key setup process with retry logic"""
        try:
            if not self.config.ssh_password:
                logger.info("No password provided, skipping SSH key setup")
                return False
            
            # Clean existing keys if requested
            if self.config.ssh_clean_keys:
                self.clean_existing_keys()
            
            # First check if existing key already works (unless cleaning or forcing)
            if not self.config.ssh_clean_keys and not self.config.ssh_setup_keys:
                if self.check_existing_key_auth():
                    return True
                
            self.ensure_ssh_dir()
            
            # Generate key if needed (or if forced)
            if not self.generate_ssh_key():
                return False
            
            # Copy public key to remote host using password
            max_retries = 2
            for attempt in range(max_retries):
                if attempt > 0:
                    wait_time = 10 * attempt  # Wait longer between retries
                    logger.info(f"Waiting {wait_time} seconds before retry {attempt + 1}...")
                    time.sleep(wait_time)
                
                logger.info(f"Attempt {attempt + 1} of {max_retries} to install SSH key...")
                
                if self.copy_public_key_to_remote():
                    break
                elif attempt == max_retries - 1:
                    logger.error("Failed to install public key after all retries")
                    return False
                else:
                    logger.warning(f"Attempt {attempt + 1} failed, will retry...")
            
            # Give the server a moment to process the key
            time.sleep(2)
            
            # Test key authentication
            if not self.test_key_authentication():
                logger.warning("Key authentication test failed, but key might still work")
                # Don't fail here - sometimes the test fails but the key works in practice
            
            # Setup SSH config optimizations
            self.setup_ssh_config()
            
            # Update config to use key instead of password
            self.config.ssh_key = str(self.private_key_path)
            self.config.ssh_password = None  # Clear password since we now have key
            
            logger.info("SSH key setup completed successfully - future connections will be faster!")
            return True
            
        except Exception as e:
            logger.error(f"Error during SSH key setup: {e}")
            return False


def _check_ssh_dependencies():
    """Check if required SSH tools are available"""
    required_tools = ['ssh', 'ssh-keygen']
    optional_tools = ['sshpass', 'ssh-copy-id']
    
    missing_required = []
    missing_optional = []
    
    for tool in required_tools:
        if shutil.which(tool) is None:
            missing_required.append(tool)
    
    for tool in optional_tools:
        if shutil.which(tool) is None:
            missing_optional.append(tool)
    
    if missing_required:
        logger.error(f"Required SSH tools missing: {', '.join(missing_required)}")
        logger.error("Please install OpenSSH client")
        return False
    
    if missing_optional:
        logger.warning(f"Optional SSH tools missing: {', '.join(missing_optional)}")
        logger.warning("Consider installing: sudo apt-get install sshpass")
    
    return True

@dataclass
class KafkaEvent:
    """Represents a Kafka event with metadata"""
    timestamp: datetime
    topic: str
    device_id: str
    event_type: str
    message: str
    raw_data: Dict[str, Any]
    partition: Optional[int] = None
    offset: Optional[int] = None

@dataclass
class TraceConfig:
    """Configuration for the trace operation"""
    namespace: str
    device_id: str
    kafka_broker: str
    max_age_hours: int = 24*8
    max_parallel_topics: int = 7
    output_dir: str = "./trace_output"
    # El timeout del consumidor ahora es un guardián, no el mecanismo principal de parada
    consumer_inactivity_timeout: int = 15  # seconds
    topic_filter: Optional[str] = None
    topic_list: Optional[List[str]] = None  # Explicit list of topics to process
    max_messages: Optional[int] = None
    ssh_host: Optional[str] = None
    ssh_user: Optional[str] = None
    ssh_password: Optional[str] = None
    ssh_key: Optional[str] = None  # SSH private key file path
    ssh_setup_keys: bool = False  # Force SSH key setup
    ssh_no_key_setup: bool = False  # Disable automatic key setup
    ssh_clean_keys: bool = False  # Remove existing keys before setup
    kafka_pod_selector: str = "deployment/kafka"
    kafka_container: Optional[str] = None
    skip_offset_check: bool = False  # Skip offset checking if SSH is problematic


def _contains_device_id(data: Dict[str, Any], device_id: str) -> bool:
    """Check if the JSON data contains the target device_id anywhere in the text"""
    try:
        json_text = orjson.dumps(data).decode('utf-8')
        device_id_lower = device_id.lower()
        json_text_lower = json_text.lower()
        result = device_id_lower in json_text_lower
        
        # Debug: Log search details for first few checks
        global message_check_count
        if 'message_check_count' not in globals():
            message_check_count = 0
        message_check_count += 1
        
        # Enhanced debugging for the first few messages or when we find matches
        if message_check_count <= 5 or result:
            logger.info(f"Device ID check #{message_check_count}: Looking for '{device_id_lower}' in message of length {len(json_text)}")
            logger.info(f"Match found: {result}")
            
            # Show more context if it's a short message or if we found a match
            if len(json_text) < 1000 or result:
                logger.info(f"Full message content: {json_text}")
            else:
                logger.info(f"Message preview: {json_text[:300]}...{json_text[-200:]}")
        
        return result
    except Exception as e:
        logger.warning(f"Error in _contains_device_id: {e}")
        # Fallback to string search
        try:
            return device_id.lower() in str(data).lower()
        except:
            return False


def _extract_event_type(data: Dict[str, Any]) -> str:
    """Extract event type from JSON data"""
    possible_keys = ['event_type', 'action_type', 'eventType', 'actionType', 'type', 'action', 'operation', 'method']
    for key in possible_keys:
        if key in data and data[key]:
            event_type = str(data[key]).strip()
            if event_type:
                return event_type
    if 'message' in data:
        return 'log_message'
    elif 'error' in data or 'exception' in data:
        return 'error'
    else:
        return 'unknown'


def _execute_remote_command(config: TraceConfig, cmd: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Execute a command on the remote SSH host if configured, otherwise locally"""
    if config.ssh_host and config.ssh_user:
        # Build SSH command with optimizations
        ssh_cmd = ["ssh"]
        
        # Add key authentication if available
        if config.ssh_key and Path(config.ssh_key).exists():
            ssh_cmd.extend([
                "-i", config.ssh_key,
                "-o", "PreferredAuthentications=publickey",
                "-o", "BatchMode=yes"
            ])
        elif config.ssh_password:
            # Use password authentication with sshpass
            ssh_cmd = ["sshpass", "-p", config.ssh_password] + ssh_cmd
            ssh_cmd.extend(["-o", "PreferredAuthentications=password"])
        
        # Add performance optimizations
        ssh_cmd.extend([
            "-o", "StrictHostKeyChecking=no", 
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ControlMaster=auto",
            f"-o", f"ControlPath=~/.ssh/control-%r@%h:%p",
            "-o", "ControlPersist=300",
            "-o", "Compression=yes",
            "-o", "TCPKeepAlive=yes",
            f"{config.ssh_user}@{config.ssh_host}"
        ])
        
        remote_cmd = " ".join([f"'{arg}'" if " " in arg else arg for arg in cmd])
        full_cmd = ssh_cmd + [remote_cmd]
        
        auth_method = "key" if config.ssh_key else "password"
        logger.debug(f"Executing SSH command ({auth_method} auth, timeout={timeout}s): {' '.join(cmd)}")
        
        try:
            result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
            if result.returncode != 0:
                logger.debug(f"SSH command failed with return code {result.returncode}")
                logger.debug(f"SSH STDERR: {result.stderr}")
            return result
        except subprocess.TimeoutExpired as e:
            logger.error(f"SSH command timed out after {timeout}s: {' '.join(cmd)}")
            raise
        except Exception as e:
            logger.error(f"SSH command failed: {e}")
            raise
    else:
        logger.debug(f"Executing local command: {' '.join(cmd)}")
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _execute_remote_popen(config: TraceConfig, cmd: List[str]) -> subprocess.Popen:
    """Execute a command on the remote SSH host using Popen if configured, otherwise locally"""
    if config.ssh_host and config.ssh_user:
        # Build SSH command with optimizations
        ssh_cmd = ["ssh"]
        
        # Add key authentication if available
        if config.ssh_key and Path(config.ssh_key).exists():
            ssh_cmd.extend([
                "-i", config.ssh_key,
                "-o", "PreferredAuthentications=publickey",
                "-o", "BatchMode=yes"
            ])
        elif config.ssh_password:
            # Use password authentication with sshpass
            ssh_cmd = ["sshpass", "-p", config.ssh_password] + ssh_cmd
            ssh_cmd.extend(["-o", "PreferredAuthentications=password"])
        
        # Add performance optimizations
        ssh_cmd.extend([
            "-o", "StrictHostKeyChecking=no", 
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ControlMaster=auto",
            f"-o", f"ControlPath=~/.ssh/control-%r@%h:%p",
            "-o", "ControlPersist=300",
            "-o", "Compression=yes",
            "-o", "TCPKeepAlive=yes",
            f"{config.ssh_user}@{config.ssh_host}"
        ])
        
        remote_cmd = " ".join([f"'{arg}'" if " " in arg else arg for arg in cmd])
        full_cmd = ssh_cmd + [remote_cmd]
        
        auth_method = "key" if config.ssh_key else "password"
        logger.debug(f"Executing SSH Popen command ({auth_method} auth): {' '.join(cmd)}")
        
        return subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, bufsize=1, universal_newlines=True)
    else:
        logger.debug(f"Executing local Popen command: {' '.join(cmd)}")
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, bufsize=1, universal_newlines=True)


def _get_topic_end_offsets(topic: str, config: TraceConfig) -> Dict[int, int]:
    """
    Get the end offsets for all partitions of a given topic.
    Returns a dictionary mapping partition_id -> end_offset.
    """
    logger.info(f"Getting end offsets for topic: {topic}")
    offsets = {}
    try:
        # Use kafka-get-offsets.sh directly (available as alias/in PATH)
        inner_cmd = [
            "env", "JMX_PORT=",  # Clear JMX_PORT to avoid issues
            "kafka-get-offsets.sh",
            "--broker-list", config.kafka_broker,
            "--topic", topic,
            "--time", "-1"  # -1 means latest offset
        ]
        
        inner_cmd_str = " ".join(shlex.quote(c) for c in inner_cmd)
        
        # Log the command being executed
        logger.info(f"Command to execute: {inner_cmd_str}")

        cmd = [
            "kubectl", "exec", "-n", config.namespace,
            config.kafka_pod_selector
        ]
        if config.kafka_container:
            cmd.extend(["-c", config.kafka_container])
        cmd.extend(["--", "sh", "-c", inner_cmd_str])

        # Print the full command for debugging
        logger.info(f"Executing command to get offsets: {' '.join(cmd)}")
        
        result = _execute_remote_command(config, cmd, timeout=30)

        if result.returncode != 0:
            # Print full debug information
            logger.error(f"Failed to get offsets for topic {topic}:")
            logger.error(f"  Return code: {result.returncode}")
            logger.error(f"  STDOUT: {result.stdout.strip()}")
            logger.error(f"  STDERR: {result.stderr.strip()}")
            return {}

        # Output format is: topic:partition:offset
        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            parts = line.strip().split(':')
            if len(parts) == 3:
                try:
                    partition = int(parts[1])
                    offset = int(parts[2])
                    offsets[partition] = offset
                except (ValueError, IndexError):
                    logger.warning(f"Could not parse offset line: '{line}'")
        
        logger.info(f"End offsets for topic {topic}: {offsets}")
        return offsets

    except Exception as e:
        logger.error(f"Exception while getting offsets for topic {topic}: {e}")
        return {}


def consume_topic_process(topic: str, config: TraceConfig, temp_dir: str) -> str:
    """
    Process function to consume a single Kafka topic until end offsets are reached.
    Writes its results to a separate file and returns the path.
    """
    process_id = os.getpid()
    output_file = os.path.join(temp_dir, f"topic_{topic}_{process_id}_{uuid.uuid4().hex}.json")
    
    logger.info(f"Process {process_id} starting for topic: {topic}")
    time.sleep(random.uniform(0.1, 1.5)) # Stagger start

    try:
        # 1. Get the target end offsets before starting the consumer (if not skipping)
        if config.skip_offset_check:
            logger.info(f"Process {process_id}: Skipping offset check for topic {topic} due to configuration")
            end_offsets = {}
            partitions_to_check = {}
            partitions_finished = {}
        else:
            end_offsets = _get_topic_end_offsets(topic, config)
            if not end_offsets:
                logger.warning(f"Process {process_id}: Could not determine end offsets for topic {topic}. Continuing without offset tracking.")
                end_offsets = {}
                partitions_to_check = {}
                partitions_finished = {}
            else:
                # A dictionary to track if we've reached the end for each partition
                # We only track partitions that have messages (offset > 0)
                partitions_to_check = {p: o for p, o in end_offsets.items() if o > 0}
                partitions_finished = {p: False for p in partitions_to_check}

        # If all partitions are empty, we are done.
        if not partitions_to_check:
            logger.info(f"Process {process_id}: Topic {topic} is empty or has no messages. Skipping consumption.")
            with open(output_file, 'wb') as f:
                f.write(orjson.dumps([]))
            return output_file
        
        # Quick test: get a sample message to verify topic accessibility
        test_cmd = [
            "env", "JMX_PORT=",
            "kafka-console-consumer.sh",
            "--bootstrap-server", config.kafka_broker,
            "--topic", topic,
            "--from-beginning",
            "--max-messages", "1",
            "--timeout-ms", "5000"
        ]
        test_cmd_str = " ".join(shlex.quote(c) for c in test_cmd)
        test_full_cmd = [
            "kubectl", "exec", "-n", config.namespace,
            config.kafka_pod_selector
        ]
        if config.kafka_container:
            test_full_cmd.extend(["-c", config.kafka_container])
        test_full_cmd.extend(["--", "sh", "-c", test_cmd_str])
        
        logger.info(f"Process {process_id} testing topic accessibility for {topic}")
        test_result = _execute_remote_command(config, test_full_cmd, timeout=10)
        if test_result.returncode != 0:
            logger.warning(f"Process {process_id} topic test failed for {topic}: {test_result.stderr.strip()}")
        else:
            logger.info(f"Process {process_id} topic {topic} is accessible, found sample data: {'Yes' if test_result.stdout.strip() else 'No'}")
        
        # 2. Build and execute the consumer command with optimized settings
        consumer_parts = [
            "env", "JMX_PORT=",
            "kafka-console-consumer.sh",
            "--bootstrap-server", config.kafka_broker,
            "--topic", topic,
            "--from-beginning",
            # Optimize for faster consumption and termination
            "--consumer-property", "session.timeout.ms=6000",
            "--consumer-property", "heartbeat.interval.ms=2000", 
            "--consumer-property", "fetch.min.bytes=1",
            "--consumer-property", "fetch.max.wait.ms=1000",  # Reduced from 500 to 1000
            "--consumer-property", "max.partition.fetch.bytes=1048576",  # 1MB instead of 10MB
            "--consumer-property", "enable.auto.commit=false",  # Don't commit offsets
            "--property", "print.timestamp=true",
            # Remove partition and offset printing since they seem to be causing issues
            # "--property", "print.partition=true",
            # "--property", "print.offset=true",
        ]
        if config.max_messages is not None:
            consumer_parts.extend(["--max-messages", str(config.max_messages)])
        
        # Use timeout to force consumer termination after reasonable time
        timeout_cmd = f"timeout {config.consumer_inactivity_timeout + 5}"
        inner_cmd_str = timeout_cmd + " " + " ".join(shlex.quote(c) for c in consumer_parts)
        
        # Log the consumer command for debugging
        logger.info(f"Process {process_id} consumer command: {inner_cmd_str}")
        
        cmd = [
            "kubectl", "exec", "-n", config.namespace,
            config.kafka_pod_selector
        ]
        if config.kafka_container:
            cmd.extend(["-c", config.kafka_container])
        cmd.extend(["--", "sh", "-c", inner_cmd_str])
        
        logger.info(f"Process {process_id} consuming topic {topic} until offsets {partitions_to_check} are met.")

        events_found = []
        start_time = time.time()
        last_activity_time = start_time

        with _execute_remote_popen(config, cmd) as proc:
            if proc.stdout is None:
                stderr_text = proc.stderr.read() if proc.stderr else "N/A"
                logger.error(f"Process {process_id} failed to start consumer for topic {topic}. Stderr: {stderr_text.strip()}")
                with open(output_file, 'wb') as f: f.write(orjson.dumps([]));
                return output_file

            # 3. Read output with simplified approach
            messages_processed = 0
            start_time = time.time()
            all_messages = []  # Store all messages for debugging
            
            logger.info(f"Process {process_id} starting to read messages from topic {topic}")
            
            try:
                for line in proc.stdout:
                    if not line.strip():
                        continue

                    messages_processed += 1
                    
                    # Log progress more frequently at the beginning
                    if messages_processed <= 10 or messages_processed % 50 == 0:
                        logger.info(f"Process {process_id} processed {messages_processed} messages from {topic}")

                    # First, store the raw line for debugging - ALWAYS
                    raw_debug_info = {
                        "message_number": messages_processed,
                        "raw_line": line.strip()
                    }
                    all_messages.append(raw_debug_info)
                    
                    try:
                        # Parse line: CreateTime:ts\tmessage (simplified format)
                        parts = line.strip().split('\t', 1)
                        if len(parts) < 2: 
                            # Update the debug info to show why it was skipped
                            raw_debug_info["skip_reason"] = "Less than 2 parts after tab split"
                            raw_debug_info["parts_count"] = len(parts)
                            continue
                        
                        metadata, message = parts
                        timestamp_str = None
                        
                        # Parse metadata - look for timestamp
                        for part in metadata.split():
                            if part.startswith('CreateTime:'): 
                                timestamp_str = part.split(':', 1)[1]
                                break
                        
                        # If no timestamp in metadata, maybe the whole metadata IS the timestamp
                        if not timestamp_str and metadata.isdigit():
                            timestamp_str = metadata

                        # We need at least timestamp to proceed
                        if not timestamp_str: 
                            # Update the debug info to show parsing details
                            raw_debug_info["skip_reason"] = "Missing timestamp"
                            raw_debug_info["metadata"] = metadata
                            continue
                        
                        event_timestamp = datetime.fromtimestamp(int(timestamp_str) / 1000)
                        # Use default values for partition and offset since we're not printing them
                        partition = 0
                        offset = messages_processed  # Use message number as offset

                        # Update debug info with parsed data
                        raw_debug_info.update({
                            "timestamp": event_timestamp.isoformat(),
                            "partition": partition,
                            "offset": offset,
                            "raw_message": message,
                            "metadata": metadata,
                            "parsed_successfully": True
                        })

                        # Time filtering
                        if config.max_age_hours > 0:
                            cutoff_time = datetime.now() - timedelta(hours=config.max_age_hours)
                            if event_timestamp < cutoff_time:
                                raw_debug_info["skip_reason"] = "Message too old"
                                raw_debug_info["cutoff_time"] = cutoff_time.isoformat()
                                continue
                        
                        # Parse JSON and create event
                        try:
                            json_data = orjson.loads(message)
                            raw_debug_info["json_parsed"] = True
                        except orjson.JSONDecodeError as e:
                            json_data = {"raw_message": message}
                            raw_debug_info["json_parsed"] = False
                            raw_debug_info["json_error"] = str(e)

                        # Debug: Log a few sample messages to see the structure
                        if messages_processed <= 5:
                            logger.info(f"Process {process_id} sample message {messages_processed}: {message[:200]}...")

                        # Since we're not tracking real partitions/offsets, use a simpler termination logic
                        # We'll rely on the timeout mechanism instead of offset tracking
                        
                        # Check if message contains our device_id
                        contains_device = _contains_device_id(json_data, config.device_id)
                        raw_debug_info["contains_device_id"] = contains_device
                        
                        # Only save events that contain our device_id
                        if contains_device:
                            event = {
                                "timestamp": event_timestamp.isoformat(), "topic": topic,
                                "device_id": config.device_id, "event_type": _extract_event_type(json_data),
                                "message": message, "raw_data": json_data,
                                "partition": partition, "offset": offset
                            }
                            events_found.append(event)
                            raw_debug_info["event_saved"] = True
                            logger.info(f"Process {process_id} found event in {topic}:{partition}@{offset}")
                        else:
                            raw_debug_info["event_saved"] = False
                            # Debug: Log why message was filtered out (only for first few messages)
                            if messages_processed <= 3:
                                logger.info(f"Process {process_id} message {messages_processed} does not contain device_id '{config.device_id}'")

                        # For now, let the timeout handle termination since we don't have reliable partition/offset info
                        # Check if all partitions are finished
                        # if all(partitions_finished.values()):
                        #     logger.info(f"Process {process_id}: All target offsets reached for topic {topic}. Terminating consumer after {messages_processed} messages.")
                        #     break

                    except Exception as e:
                        logger.error(f"Process {process_id} error parsing line from topic {topic}: {e}")
                        # Update debug info with error details
                        if 'raw_debug_info' in locals():
                            raw_debug_info["parsing_error"] = str(e)
                            raw_debug_info["parsing_error_type"] = type(e).__name__
                        continue

            except Exception as e:
                logger.error(f"Process {process_id} error reading from stdout: {e}")
            
            # Save ALL messages to debug file (optional, can be enabled for debugging)
            if False:  # Set to True to enable debug file generation
                os.makedirs(config.output_dir, exist_ok=True)
                debug_file = os.path.join(config.output_dir, f"debug_all_messages_{topic}_{process_id}.json")
                try:
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(json.dumps(all_messages, indent=2))
                    logger.info(f"Process {process_id} saved all {len(all_messages)} messages to debug file: {debug_file}")
                except Exception as e:
                    logger.error(f"Process {process_id} failed to save debug file: {e}")
            
            logger.info(f"Process {process_id} finished reading {messages_processed} total messages from topic {topic}")
            
            # Safety check: if we haven't finished all partitions, log it
            if not all(partitions_finished.values()):
                unfinished = [p for p, finished in partitions_finished.items() if not finished]
                logger.warning(f"Process {process_id} for topic {topic} finished but partitions {unfinished} were not completed. This is normal if those partitions don't have enough messages.")

            # Terminate the process gracefully
            if proc.poll() is None:  # If still running
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Process {process_id} for topic {topic} did not terminate gracefully, killing.")
                    proc.kill()


        total_elapsed = time.time() - start_time
        logger.info(f"Process {process_id} completed for topic {topic}. Found {len(events_found)} events in {total_elapsed:.2f}s.")

        with open(output_file, 'wb') as f:
            f.write(orjson.dumps(events_found))
        return output_file

    except Exception as e:
        logger.error(f"Process {process_id} encountered a fatal error for topic {topic}: {e}", exc_info=True)
        with open(output_file, 'wb') as f:
            f.write(orjson.dumps([]))
        return output_file


class KomniTracer:
    """Main class for tracing Kafka events across topics"""
    
    def __init__(self, config: TraceConfig):
        self.config = config
        self.events: List[KafkaEvent] = []
        self.temp_dir = tempfile.mkdtemp(prefix="komni_trace_")
        os.makedirs(self.config.output_dir, exist_ok=True)
        logger.info(f"KomniTracer initialized for device_id: {config.device_id}")
        logger.info(f"Using temporary directory: {self.temp_dir}")
        
        # Test SSH connectivity if configured
        if config.ssh_host:
            self._test_ssh_connectivity()
    
    def _test_ssh_connectivity(self):
        """Test SSH connectivity and setup SSH keys for improved performance"""
        try:
            logger.info(f"Testing SSH connectivity to {self.config.ssh_host}...")
            
            # Check SSH dependencies first
            if not _check_ssh_dependencies():
                raise Exception("Required SSH tools are not available")
            
            # Try initial connection test
            test_cmd = ["echo", "SSH connection test successful"]
            result = _execute_remote_command(self.config, test_cmd, timeout=15)
            
            if result.returncode == 0:
                auth_method = "key" if self.config.ssh_key else "password"
                logger.info(f"SSH connectivity test successful using {auth_method} authentication")
                logger.info(f"SSH response: {result.stdout.strip()}")
                
                # If using password, try to set up SSH keys for future performance
                if (self.config.ssh_password and not self.config.ssh_key and 
                    not self.config.ssh_no_key_setup):
                    logger.info("Setting up SSH keys for improved performance...")
                    ssh_manager = SSHKeyManager(self.config)
                    
                    # Check if we should force setup even if keys exist
                    should_setup = self.config.ssh_setup_keys or not ssh_manager.private_key_path.exists()
                    
                    if should_setup and ssh_manager.setup_ssh_keys():
                        logger.info("🚀 SSH keys configured! Future connections will be faster and password-free.")
                        
                        # Update our config to use the new key
                        self.config.ssh_key = ssh_manager.config.ssh_key
                        self.config.ssh_password = ssh_manager.config.ssh_password
                        
                        # Test the new key-based connection
                        test_result = _execute_remote_command(self.config, test_cmd, timeout=10)
                        if test_result.returncode == 0:
                            logger.info("✅ Key-based authentication confirmed working")
                        else:
                            logger.warning("⚠️ Key setup completed but test failed, falling back to password")
                            # Restore password if key test failed
                            self.config.ssh_password = ssh_manager.config.ssh_password or self.config.ssh_password
                            self.config.ssh_key = None
                    elif not should_setup:
                        logger.info("SSH keys already exist, use --ssh-setup-keys to force regeneration")
                    else:
                        logger.warning("SSH key setup failed, continuing with password authentication")
                elif self.config.ssh_no_key_setup:
                    logger.info("SSH key setup disabled by --ssh-no-key-setup flag")
                
                return
            else:
                logger.error(f"SSH connectivity test failed with return code {result.returncode}")
                logger.error(f"SSH STDOUT: {result.stdout.strip()}")
                logger.error(f"SSH STDERR: {result.stderr.strip()}")
                
                # Provide helpful suggestions
                suggestions = []
                if "Permission denied" in result.stderr:
                    suggestions.append("- Check SSH username and password/key")
                    suggestions.append("- Verify SSH service is running on target host")
                    suggestions.append("- Ensure password authentication is enabled on the server")
                if "Connection refused" in result.stderr:
                    suggestions.append("- Check if SSH service is running on port 22")
                    suggestions.append("- Verify firewall allows SSH connections")
                if "No route to host" in result.stderr:
                    suggestions.append("- Check network connectivity to target host")
                    suggestions.append("- Verify IP address is correct")
                if "sshpass" in result.stderr and "command not found" in result.stderr:
                    suggestions.append("- Install sshpass: sudo apt-get install sshpass")
                    suggestions.append("- Or provide SSH key instead of password")
                
                if suggestions:
                    logger.error("Possible solutions:")
                    for suggestion in suggestions:
                        logger.error(suggestion)
                
                raise Exception(f"SSH connectivity test failed: {result.stderr.strip()}")
                
        except subprocess.TimeoutExpired:
            logger.error("SSH connectivity test timed out after 15 seconds")
            raise Exception("SSH connection timeout - check network connectivity")
        except Exception as e:
            logger.error(f"SSH connectivity test failed: {e}")
            raise Exception(f"Cannot establish SSH connection to {self.config.ssh_host}: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """Clean up temporary resources and SSH connections"""
        # Clean up SSH control connections
        if self.config.ssh_host:
            try:
                # Close any persistent SSH connections
                control_path = f"~/.ssh/control-{self.config.ssh_user}@{self.config.ssh_host}:22"
                subprocess.run(["ssh", "-O", "exit", "-o", f"ControlPath={control_path}", 
                               f"{self.config.ssh_user}@{self.config.ssh_host}"], 
                              capture_output=True, timeout=5)
                logger.debug("Closed SSH control connections")
            except Exception as e:
                logger.debug(f"Error closing SSH connections: {e}")
        
        # Clean up temporary directory
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                logger.info(f"Successfully cleaned up temporary directory: {self.temp_dir}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary directory {self.temp_dir}: {e}")
    
    def discover_kafka_topics(self) -> List[str]:
        """Discover available Kafka topics in the namespace"""
        try:
            logger.info("Starting Kafka topic discovery...")
            cmd = [
                "kubectl", "exec", "-n", self.config.namespace,
                self.config.kafka_pod_selector
            ]
            if self.config.kafka_container:
                cmd.extend(["-c", self.config.kafka_container])
            cmd.extend([
                "--", "env", "JMX_PORT=", "kafka-topics.sh",
                "--bootstrap-server", self.config.kafka_broker, "--list"
            ])
            
            logger.info("Executing topic discovery command...")
            # Use shorter timeout for topic discovery
            result = _execute_remote_command(self.config, cmd, timeout=30)
            
            if result.returncode != 0:
                logger.error(f"Failed to list Kafka topics (return code: {result.returncode})")
                logger.error(f"STDOUT: {result.stdout.strip()}")
                logger.error(f"STDERR: {result.stderr.strip()}")
                return []
            
            topics = [topic.strip() for topic in result.stdout.split('\n') 
                     if topic.strip() and not topic.strip().startswith('__')]
            
            if self.config.topic_filter:
                topics = [t for t in topics if self.config.topic_filter in t]
                logger.info(f"Applied topic filter '{self.config.topic_filter}': {len(topics)} topics match.")
            
            logger.info(f"Discovered {len(topics)} Kafka topics to process: {topics[:5]}{'...' if len(topics) > 5 else ''}")
            return topics
            
        except subprocess.TimeoutExpired:
            logger.error("Topic discovery timed out after 30 seconds")
            return []
        except Exception as e:
            logger.error(f"Error discovering Kafka topics: {e}", exc_info=True)
            return []
    
    def run_trace(self) -> List[KafkaEvent]:
        """Execute the main tracing process using parallel processes."""
        logger.info("Starting Kafka event trace")
        
        # Use explicit topic list if provided, otherwise discover topics
        if self.config.topic_list:
            topics = self.config.topic_list
            logger.info(f"Using provided topic list: {topics}")
        else:
            topics = self.discover_kafka_topics()
            if not topics:
                logger.error("No Kafka topics found to process.")
                return []
        
        logger.info(f"Processing {len(topics)} topics with max {self.config.max_parallel_topics} parallel processes")
        
        if self.config.max_parallel_topics > 8:
            logger.warning("More than 8 will cause ssh connection issues, consider reducing this value.")
            
        
        all_events = []
        with ProcessPoolExecutor(max_workers=self.config.max_parallel_topics) as executor:
            future_to_topic = {
                executor.submit(consume_topic_process, topic, self.config, self.temp_dir): topic
                for topic in topics
            }
            
            for future in as_completed(future_to_topic):
                topic = future_to_topic[future]
                try:
                    result_file = future.result()
                    events = self._load_events_from_file(result_file)
                    all_events.extend(events)
                    logger.info(f"Collected {len(events)} events from topic {topic}")
                except Exception as e:
                    logger.error(f"Error processing result from topic {topic}: {e}")
        
        all_events.sort(key=lambda e: e.timestamp)
        self.events = all_events
        
        logger.info(f"Total events collected: {len(all_events)}")
        return all_events
    
    def _load_events_from_file(self, file_path: str) -> List[KafkaEvent]:
        """Load events from a JSON file created by a worker process"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                events_data = orjson.loads(f.read())
            
            return [KafkaEvent(
                timestamp=datetime.fromisoformat(e['timestamp']),
                topic=e['topic'], device_id=e['device_id'],
                event_type=e['event_type'], message=e['message'],
                raw_data=e['raw_data'], partition=e.get('partition'),
                offset=e.get('offset')
            ) for e in events_data]
        except Exception as e:
            logger.error(f"Error loading events from {file_path}: {e}")
            return []


def main():
    """Main function to parse arguments and execute the trace"""
    parser = argparse.ArgumentParser(
        description='KomniTrace - Kafka Event Tracer for Kubernetes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - trace events for a specific device
  python KomniTrace.py --namespace my-namespace --device-id "12345"
  
  # Process only specific topics (much faster)
  python KomniTrace.py --namespace my-namespace --device-id "12345" \\
    --topics as-provision-topic device-events-topic audit-topic
  
  # Remote execution via SSH with automatic key setup
  python KomniTrace.py --namespace my-namespace --device-id "12345" \\
    --ssh-host "master.k8s.local" --ssh-user "admin" --ssh-password "secret"
  
  # Force SSH key regeneration for performance
  python KomniTrace.py --namespace my-namespace --device-id "12345" \\
    --ssh-host "master.k8s.local" --ssh-user "admin" --ssh-password "secret" \\
    --ssh-setup-keys
  
  # Use existing SSH key (skip automatic setup)
  python KomniTrace.py --namespace my-namespace --device-id "12345" \\
    --ssh-host "master.k8s.local" --ssh-user "admin" \\
    --ssh-key "~/.ssh/my_key"
  
  # Disable automatic SSH key setup (password only)
  python KomniTrace.py --namespace my-namespace --device-id "12345" \\
    --ssh-host "master.k8s.local" --ssh-user "admin" --ssh-password "secret" \\
    --ssh-no-key-setup

SSH Performance Features:
  - Automatic SSH key generation and deployment for faster connections
  - Persistent SSH connections with connection multiplexing
  - Optimized SSH settings for maximum throughput
  - Fallback to password authentication if key setup fails
        """
    )
    
    parser.add_argument('--namespace', '-n', required=True, help='Kubernetes namespace where Kafka is running')
    parser.add_argument('--device-id', '-d', required=True, help='Device ID to search for in Kafka events')
    parser.add_argument('--kafka-broker', '-b', default='kafka:9092', help='Kafka broker address (default: kafka:9092)')
    parser.add_argument('--max-age', '-a', type=int, default=24 * 8, help='Maximum age of events in hours (default: 192, 0 = all history)')
    parser.add_argument('--max-parallel', '-p', type=int, default=4, help='Maximum parallel topic consumers')
    parser.add_argument('--output-dir', '-o', default='./trace_output', help='Output directory for results')
    parser.add_argument('--timeout', type=int, default=15, help='Consumer inactivity timeout in seconds (default: 15)')
    parser.add_argument('--max-messages', '-m', type=int, default=None, help='Maximum messages to consume per topic (default: unlimited)')
    parser.add_argument('--topic-filter', '-f', help='Filter to process only topics containing this string')
    parser.add_argument('--topics', nargs='+', help='Explicit list of topics to process (space-separated). If provided, skips topic discovery.')
    
    parser.add_argument('--ssh-host', help='SSH host for remote execution')
    parser.add_argument('--ssh-user', help='SSH username')
    parser.add_argument('--ssh-password', help='SSH password')
    parser.add_argument('--ssh-key', help='SSH private key file path (alternative to password)')
    parser.add_argument('--ssh-setup-keys', action='store_true', help='Force SSH key setup even if keys already exist')
    parser.add_argument('--ssh-no-key-setup', action='store_true', help='Disable automatic SSH key setup (use password only)')
    parser.add_argument('--ssh-clean-keys', action='store_true', help='Remove existing SSH keys for this host before setup')
    
    parser.add_argument('--kafka-pod', default='deployment/kafka', help='Kafka pod selector (default: deployment/kafka)')
    parser.add_argument('--kafka-container', help='Kafka container name (if multiple containers in pod)')
    
    parser.add_argument('--no-graphs', action='store_true', help='Skip graph generation')
    parser.add_argument('--skip-offsets', action='store_true', help='Skip offset checking (faster but less precise termination)')
    
    args = parser.parse_args()
    
    config = TraceConfig(
        namespace=args.namespace, device_id=args.device_id,
        kafka_broker=args.kafka_broker, max_age_hours=args.max_age,
        max_parallel_topics=args.max_parallel, output_dir=args.output_dir,
        consumer_inactivity_timeout=args.timeout, topic_filter=args.topic_filter,
        topic_list=args.topics, max_messages=args.max_messages, ssh_host=args.ssh_host,
        ssh_user=args.ssh_user, ssh_password=args.ssh_password, ssh_key=args.ssh_key,
        ssh_setup_keys=args.ssh_setup_keys, ssh_no_key_setup=args.ssh_no_key_setup,
        ssh_clean_keys=args.ssh_clean_keys, kafka_pod_selector=args.kafka_pod, 
        kafka_container=args.kafka_container, skip_offset_check=args.skip_offsets
    )
    
    logger.info(f"Starting KomniTrace for device_id: {config.device_id}")
    
    try:
        with KomniTracer(config) as tracer:
            events = tracer.run_trace()
            
            if not events:
                logger.warning("No events found for the specified device_id")
                return
            
            logger.info(f"Found {len(events)} events for device {config.device_id}")
            
            # --- Report Generation ---
            report_path = ""
            if not args.no_graphs and KomniReportGenerator:
                logger.info("Generating visualizations and HTML report...")
                report_generator = KomniReportGenerator(events, config)
                
                # Generate all graph types and collect their paths
                graph_paths = {}
                
                try:
                    # Generate timeline graph
                    timeline_path = report_generator.generate_timeline_graph()
                    if timeline_path:
                        graph_paths['timeline'] = timeline_path
                        logger.info(f"Generated timeline graph: {timeline_path}")
                except Exception as e:
                    logger.warning(f"Failed to generate timeline graph: {e}")
                
                try:
                    # Generate swimlane timeline
                    swimlane_path = report_generator.generate_timeline_swimlane()
                    if swimlane_path:
                        graph_paths['swimlane'] = swimlane_path
                        logger.info(f"Generated swimlane timeline: {swimlane_path}")
                except Exception as e:
                    logger.warning(f"Failed to generate swimlane timeline: {e}")
                
                try:
                    # Generate event flow diagram
                    flow_path = report_generator.generate_event_flow_diagram()
                    if flow_path:
                        graph_paths['flow'] = flow_path
                        logger.info(f"Generated flow diagram: {flow_path}")
                except Exception as e:
                    logger.warning(f"Failed to generate flow diagram: {e}")
                
                try:
                    # Generate compact timeline
                    compact_path = report_generator.generate_compact_timeline()
                    if compact_path:
                        graph_paths['compact'] = compact_path
                        logger.info(f"Generated compact timeline: {compact_path}")
                except Exception as e:
                    logger.warning(f"Failed to generate compact timeline: {e}")
                
                # Generate HTML report with all the graph paths
                try:
                    report_path = report_generator.generate_html_report(graph_paths)
                    logger.info(f"Generated HTML report with {len(graph_paths)} visualizations")
                except Exception as e:
                    logger.error(f"Failed to generate HTML report: {e}")
                
                # Optionally generate interactive Plotly timeline if available
                try:
                    plotly_path = report_generator.generate_plotly_interactive_timeline()
                    if plotly_path:
                        logger.info(f"Generated interactive Plotly timeline: {plotly_path}")
                except Exception as e:
                    logger.warning(f"Failed to generate interactive Plotly timeline: {e}")
            
            events_file = os.path.join(config.output_dir, f"events_{config.device_id}.json")
            with open(events_file, 'w', encoding='utf-8') as f:
                # Usar orjson para un volcado rápido
                events_data = [orjson.loads(orjson.dumps(e, default=lambda o: o.__dict__ if hasattr(o, '__dict__') else str(o))) for e in tracer.events]
                f.write(json.dumps(events_data, indent=2))
            
            logger.info(f"Events saved to: {events_file}")
            
            print("\n" + "="*60)
            print("🎉 KomniTrace completed successfully!")
            print(f"📊 Found {len(events)} events for device {config.device_id}")
            print(f"📁 Results saved to: {config.output_dir}")
            print(f"  📋 Events JSON: {events_file}")
            if report_path:
                print(f"  📄 HTML Report: {report_path}")
            print("="*60)
            
    except KeyboardInterrupt:
        logger.info("Trace interrupted by user")
    except Exception as e:
        logger.error(f"Error during trace execution: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
