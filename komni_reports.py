#!/usr/bin/env python3
"""
KomniReports - Report and Visualization Generator for KomniTrace

This module contains all the report generation and visualization functionality
for the KomniTrace Kafka event tracer.

Features:
- Timeline graphs with matplotlib
- Interactive visualizations with Plotly
- HTML reports with embedded charts
- Multiple visualization types (timeline, swimlane, flow, compact)
"""

import os
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

# Optional matplotlib import
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import MaxNLocator
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Optional interactive visualization libraries
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import plotly.offline as pyo
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    import bokeh.plotting as bk
    from bokeh.models import HoverTool, ColumnDataSource
    from bokeh.layouts import column, row
    from bokeh.io import output_file, save
    BOKEH_AVAILABLE = True
except ImportError:
    BOKEH_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

logger = logging.getLogger(__name__)


class KomniReportGenerator:
    """Main class for generating reports and visualizations from KomniTrace events"""
    
    def __init__(self, events: List[Any], config: Any):
        """
        Initialize the report generator
        
        Args:
            events: List of KafkaEvent objects
            config: TraceConfig object with configuration settings
        """
        self.events = events
        self.config = config
    
    def _smart_time_axis_setup(self, ax, time_span_seconds):
        """Setup smart time axis formatting and locators to avoid tick generation issues"""
        
        # Force reasonable limits to prevent matplotlib tick explosion
        max_ticks = 10  # Maximum number of ticks allowed
        
        try:
            # Use MaxNLocator as primary locator to strictly limit ticks
            ax.xaxis.set_major_locator(MaxNLocator(nbins=max_ticks))
            
            # Then set appropriate formatter based on time span
            if time_span_seconds < 3600:  # Less than 1 hour
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            elif time_span_seconds < 86400:  # Less than 1 day  
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            elif time_span_seconds < 86400 * 7:  # Less than 1 week
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            elif time_span_seconds < 86400 * 30:  # Less than 1 month
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            else:  # More than 1 month - use conservative approach
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            
        except Exception as e:
            # Fallback to minimal ticks if there's any issue
            logger.warning(f"Time axis setup failed, using fallback: {e}")
            ax.xaxis.set_major_locator(MaxNLocator(nbins=5))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        
        plt.xticks(rotation=45, ha='right')
    
    def _create_nonlinear_time_mapping(self, timestamps):
        """Create a non-linear time mapping that compresses large gaps and expands dense areas"""
        if len(timestamps) <= 1:
            return timestamps, timestamps
        
        sorted_timestamps = sorted(timestamps)
        
        # Calculate gaps between consecutive events
        gaps = []
        for i in range(1, len(sorted_timestamps)):
            gap = (sorted_timestamps[i] - sorted_timestamps[i-1]).total_seconds()
            gaps.append(gap)
        
        if not gaps:
            return timestamps, timestamps
        
        # Define gap categories based on distribution
        gaps_sorted = sorted(gaps)
        median_gap = gaps_sorted[len(gaps_sorted)//2]
        large_gap_threshold = median_gap * 3  # Gaps 3x larger than median are "large"
        
        # Create non-linear mapping
        mapped_positions = [0]  # First event at position 0
        current_position = 0
        
        for i, gap in enumerate(gaps):
            if gap > large_gap_threshold:
                # Large gap: compress it (use logarithmic compression)
                compressed_gap = median_gap + (gap - large_gap_threshold) ** 0.3
                current_position += compressed_gap / 60  # Convert to "minutes" for positioning
            else:
                # Small/normal gap: use linear mapping but with slight expansion for very small gaps
                if gap < median_gap * 0.5:  # Very small gaps get slight expansion
                    expanded_gap = gap * 1.5
                    current_position += expanded_gap / 60
                else:
                    current_position += gap / 60
            
            mapped_positions.append(current_position)
        
        # Convert back to datetime-like objects for matplotlib
        # Use the first timestamp as base and add the mapped positions as minutes
        base_time = sorted_timestamps[0]
        mapped_timestamps = []
        
        for pos in mapped_positions:
            mapped_time = base_time + timedelta(minutes=pos)
            mapped_timestamps.append(mapped_time)
        
        # Create mapping dictionary for all original timestamps
        time_mapping = {}
        for orig_time, mapped_time in zip(sorted_timestamps, mapped_timestamps):
            time_mapping[orig_time] = mapped_time
        
        # Map all input timestamps (preserving original order)
        mapped_result = [time_mapping[ts] for ts in timestamps]
        
        return mapped_result, sorted_timestamps
    
    def generate_timeline_graph(self) -> str:
        """Generate a timeline graph using matplotlib (if available)"""
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available, skipping graph generation")
            logger.info("To enable graph generation, install matplotlib: pip install matplotlib")
            return ""
        
        if not self.events:
            logger.warning("No events to plot")
            return ""
        
        logger.info("Generating timeline graph")
        
        # Group events by topic and event_type for better visualization
        topic_events = defaultdict(lambda: defaultdict(list))
        for event in self.events:
            topic_events[event.topic][event.event_type].append(event)
        
        # Debug: Print the grouping
        logger.info("Event grouping by topic and event_type:")
        for topic, event_types in topic_events.items():
            for event_type, events in event_types.items():
                logger.info(f"  {topic} -> {event_type}: {len(events)} events")
        
        # Create figure and axis with better size
        fig, ax = plt.subplots(figsize=(20, 12))
        
        # Use different colors for topics and distinct markers/shades for event types
        topic_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*', 'h', '+', 'x']
        y_positions = {}
        current_y = 0
        
        all_timestamps = []
        legend_elements = []
        
        # Calculate spacing for nested event types within topics
        max_event_types = max(len(event_types) for event_types in topic_events.values()) if topic_events else 1
        y_spacing = 0.8 / max(max_event_types, 1)  # Space event types within 0.8 units
        
        # Plot events for each topic with nested event types
        for topic_idx, (topic, event_types) in enumerate(topic_events.items()):
            # Get base color for this topic
            base_color = topic_colors[topic_idx % len(topic_colors)]
            
            # Track total events for this topic
            total_topic_events = sum(len(events) for events in event_types.values())
            
            # Calculate Y positions for event types within this topic
            num_event_types = len(event_types)
            if num_event_types == 1:
                # Single event type, center it on the topic line
                event_y_positions = [current_y]
            else:
                # Multiple event types, distribute them around the topic line
                start_y = current_y - (y_spacing * (num_event_types - 1) / 2)
                event_y_positions = [start_y + (i * y_spacing) for i in range(num_event_types)]
            
            # Plot each event type for this topic
            for event_type_idx, (event_type, events) in enumerate(event_types.items()):
                timestamps = [event.timestamp for event in events]
                
                # Apply non-linear time mapping for better visualization
                mapped_timestamps, original_sorted = self._create_nonlinear_time_mapping(timestamps)
                all_timestamps.extend(mapped_timestamps)
                
                # Get Y position for this event type
                y_pos = event_y_positions[event_type_idx]
                y_positions_list = [y_pos] * len(mapped_timestamps)
                
                # Use different markers and alpha for different event types
                marker = markers[event_type_idx % len(markers)]
                
                # Create variations in shade and size for event types
                if num_event_types == 1:
                    alpha = 0.8
                    size = 120
                else:
                    alpha = 0.9 - (event_type_idx * 0.15)
                    alpha = max(alpha, 0.4)  # Ensure minimum visibility
                    size = 100 + (event_type_idx * 20)  # Vary size slightly
                
                scatter = ax.scatter(mapped_timestamps, y_positions_list, 
                          color=base_color, marker=marker, alpha=alpha, 
                          s=size, edgecolors='black', linewidth=0.8,
                          label=f'{event_type} ({len(events)})')
                
                legend_elements.append(scatter)
                
                # Add event type annotation next to the points with original times
                if timestamps:
                    # Position annotation at the rightmost event or middle if many events
                    if len(timestamps) == 1:
                        annotation_idx = 0
                    else:
                        # Use the last timestamp for annotation positioning
                        annotation_idx = -1
                    
                    # Show both mapped position and original time in annotation
                    orig_time = timestamps[annotation_idx]
                    mapped_time = mapped_timestamps[annotation_idx]
                    
                    ax.annotate(f'{event_type} ({len(events)})\n{orig_time.strftime("%H:%M:%S")}', 
                               (mapped_time, y_pos), 
                               xytext=(8, 0), textcoords='offset points',
                               fontsize=8, fontweight='bold', 
                               bbox=dict(boxstyle='round,pad=0.2', facecolor=base_color, alpha=0.2),
                               ha='left', va='center')
            
            # Add topic label on the left side
            topic_short = topic.replace('dp-', '').replace('-topic', '')
            if all_timestamps:  # Use all_timestamps which now contains mapped timestamps
                ax.annotate(f'{topic_short}\n({total_topic_events} events)', 
                           (min(all_timestamps), current_y), 
                           xytext=(-120, 0), textcoords='offset points',
                           fontsize=10, fontweight='bold', 
                           bbox=dict(boxstyle='round,pad=0.4', facecolor=base_color, alpha=0.3),
                           ha='center', va='center')
            
            # Draw a light vertical span to represent the topic grouping
            if all_timestamps:
                # Get all mapped timestamps for this topic
                topic_mapped_timestamps = []
                for events in event_types.values():
                    topic_timestamps = [e.timestamp for e in events]
                    mapped_topic_ts, _ = self._create_nonlinear_time_mapping(topic_timestamps)
                    topic_mapped_timestamps.extend(mapped_topic_ts)
                
                if topic_mapped_timestamps:
                    min_time = min(topic_mapped_timestamps)
                    max_time = max(topic_mapped_timestamps)
                    y_min = min(event_y_positions) - 0.2
                    y_max = max(event_y_positions) + 0.2
                    
                    # Draw vertical span to show topic area
                    ax.axhspan(y_min, y_max, alpha=0.1, color=base_color, zorder=0)
            
            y_positions[topic] = current_y
            current_y += 1.2  # Increased spacing between topics
        
        # Customize the plot
        ax.set_xlabel('Time (Non-linear scale: compressed gaps, expanded clusters)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Kafka Topics', fontsize=12, fontweight='bold')
        ax.set_title(f'Event Timeline for Device ID: {self.config.device_id}\nTotal Events: {len(self.events)} (Non-linear time scale)', 
                    fontsize=14, fontweight='bold', pad=20)
        
        # Smart time formatting based on time range (using mapped timestamps)
        if all_timestamps:
            time_span = (max(all_timestamps) - min(all_timestamps)).total_seconds()
            self._smart_time_axis_setup(ax, time_span)
        
        # Set y-axis labels for topics only (since event types are annotated)
        topic_y_positions = list(y_positions.values())
        topic_names = [topic.replace('dp-', '').replace('-topic', '') for topic in y_positions.keys()]
        ax.set_yticks(topic_y_positions)
        ax.set_yticklabels(topic_names, fontsize=11, fontweight='bold')
        
        # Add legend showing only event types (not topics, since they're on Y axis)
        if len(legend_elements) <= 20:  # Show event type legend if reasonable number
            legend = ax.legend(handles=legend_elements, bbox_to_anchor=(1.02, 1), loc='upper left', 
                             fontsize=9, title='Event Types', title_fontsize=10)
            legend.get_title().set_fontweight('bold')
        
        # Add grid with better styling - horizontal lines to separate topics
        ax.grid(True, alpha=0.3, linestyle='--', axis='x')
        
        # Add horizontal grid lines to separate topics clearly
        for y_pos in topic_y_positions:
            ax.axhline(y=y_pos, color='gray', linestyle='-', alpha=0.2, linewidth=1)
        
        # Add statistical information as text box with non-linear time info
        if all_timestamps and self.events:
            # Get original time span for statistics
            original_timestamps = [event.timestamp for event in self.events]
            time_span = max(original_timestamps) - min(original_timestamps)
            total_topics = len(topic_events)
            total_event_types = sum(len(event_types) for event_types in topic_events.values())
            
            stats_text = (f'📊 Timeline Statistics\n'
                         f'Time Span: {time_span}\n'
                         f'Topics: {total_topics}\n'
                         f'Event Types: {total_event_types}\n'
                         f'First: {min(original_timestamps).strftime("%m-%d %H:%M:%S")}\n'
                         f'Last: {max(original_timestamps).strftime("%m-%d %H:%M:%S")}\n'
                         f'⚡ Non-linear scale active:\n'
                         f'• Large gaps compressed\n'
                         f'• Event clusters expanded')
            
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                   fontsize=9, verticalalignment='top',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgreen', alpha=0.8))
        
        # Add some padding around the plot
        ax.margins(x=0.02, y=0.1)
        
        # Adjust layout to prevent cutoff
        plt.tight_layout()
        
        # Save the plot
        graph_path = os.path.join(self.config.output_dir, f"timeline_{self.config.device_id}.png")
        plt.savefig(graph_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Timeline graph saved to: {graph_path}")
        return graph_path
    
    def generate_timeline_swimlane(self) -> str:
        """Generate a swimlane timeline visualization - ideal for event sequences"""
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available, skipping swimlane generation")
            return ""
        
        if not self.events:
            logger.warning("No events to plot")
            return ""
        
        logger.info("Generating swimlane timeline visualization")
        
        # Group events by topic and event_type
        topic_events = defaultdict(lambda: defaultdict(list))
        for event in self.events:
            topic_events[event.topic][event.event_type].append(event)
        
        # Create figure with horizontal layout
        fig, ax = plt.subplots(figsize=(24, 8))
        
        # Define colors for topics and event types
        topic_colors = {'dp-provision-events-topic': '#3498db', 
                       'dp-notifications-topic': '#e74c3c', 
                       'dp-provision-upstream-events-topic': '#2ecc71'}
        default_colors = ['#f39c12', '#9b59b6', '#34495e', '#e67e22', '#1abc9c']
        
        # Timeline setup with non-linear mapping
        all_timestamps = [event.timestamp for event in self.events]
        if not all_timestamps:
            return ""
        
        # Apply non-linear time mapping
        mapped_timestamps, original_sorted = self._create_nonlinear_time_mapping(all_timestamps)
        
        min_time = min(mapped_timestamps)
        max_time = max(mapped_timestamps)
        time_span = (max_time - min_time).total_seconds()
        
        # Create horizontal timeline
        y_level = 0.5
        
        # Draw main timeline
        ax.plot([min_time, max_time], [y_level, y_level], 'k-', linewidth=3, alpha=0.3)
        
        # Create mapping for individual events
        event_time_map = {}
        for orig_time, mapped_time in zip(original_sorted, mapped_timestamps[:len(original_sorted)]):
            event_time_map[orig_time] = mapped_time
        
        # Plot events as vertical bars with annotations
        for i, event in enumerate(sorted(self.events, key=lambda x: x.timestamp)):
            # Get color for this topic
            if event.topic in topic_colors:
                color = topic_colors[event.topic]
            else:
                color = default_colors[hash(event.topic) % len(default_colors)]
            
            # Get mapped timestamp for this event
            mapped_event_time = event_time_map.get(event.timestamp, event.timestamp)
            
            # Draw vertical line for event
            ax.axvline(x=mapped_event_time, color=color, alpha=0.7, linewidth=2)
            
            # Alternate annotation positions to avoid overlap
            y_offset = 0.1 + (0.1 * (i % 3))
            if i % 2 == 0:
                y_pos = y_level + y_offset
                va = 'bottom'
            else:
                y_pos = y_level - y_offset
                va = 'top'
            
            # Add event annotation with original time
            topic_short = event.topic.replace('dp-', '').replace('-topic', '')
            ax.annotate(f'{topic_short}\n{event.event_type}\n{event.timestamp.strftime("%H:%M:%S")}', 
                       (mapped_event_time, y_pos),
                       rotation=45, fontsize=8, ha='left', va=va,
                       bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.6))
        
        # Customize plot
        ax.set_xlabel('Time (Non-linear: gaps compressed, clusters expanded)', fontsize=12, fontweight='bold')
        ax.set_title(f'Swimlane Timeline for Device ID: {self.config.device_id}\nTotal Events: {len(self.events)} (Non-linear scale)', 
                    fontsize=14, fontweight='bold')
        
        # Format time axis with smart setup
        self._smart_time_axis_setup(ax, time_span)
        
        # Remove y-axis as it's not needed
        ax.set_yticks([])
        ax.set_ylim(0, 1)
        
        # Add grid only for time
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save
        swimlane_path = os.path.join(self.config.output_dir, f"swimlane_{self.config.device_id}.png")
        plt.savefig(swimlane_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Swimlane timeline saved to: {swimlane_path}")
        return swimlane_path
    
    def generate_event_flow_diagram(self) -> str:
        """Generate a flow diagram showing event progression - ideal for understanding sequences"""
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available, skipping flow diagram generation")
            return ""
        
        if not self.events:
            logger.warning("No events to plot")
            return ""
        
        logger.info("Generating event flow diagram")
        
        # Sort events by timestamp
        sorted_events = sorted(self.events, key=lambda x: x.timestamp)
        
        # Create figure
        fig, ax = plt.subplots(figsize=(20, 12))
        
        # Define positions and colors
        topic_colors = {
            'dp-provision-events-topic': '#3498db',
            'dp-notifications-topic': '#e74c3c', 
            'dp-provision-upstream-events-topic': '#2ecc71'
        }
        
        # Calculate positions
        num_events = len(sorted_events)
        x_positions = list(range(num_events))
        
        # Group events to show flow
        for i, event in enumerate(sorted_events):
            color = topic_colors.get(event.topic, '#95a5a6')
            
            # Draw event box
            rect = plt.Rectangle((i-0.4, 0), 0.8, 1, 
                               facecolor=color, alpha=0.7, edgecolor='black')
            ax.add_patch(rect)
            
            # Add event text
            topic_short = event.topic.replace('dp-', '').replace('-topic', '')
            event_text = f'{topic_short}\n{event.event_type}\n{event.timestamp.strftime("%H:%M:%S")}'
            ax.text(i, 0.5, event_text, ha='center', va='center', fontsize=8, fontweight='bold')
            
            # Draw arrow to next event
            if i < num_events - 1:
                ax.arrow(i+0.4, 0.5, 0.2, 0, head_width=0.1, head_length=0.05, 
                        fc='gray', ec='gray', alpha=0.6)
        
        # Customize plot
        ax.set_xlim(-0.5, num_events-0.5)
        ax.set_ylim(-0.2, 1.2)
        ax.set_xlabel('Event Sequence', fontsize=12, fontweight='bold')
        ax.set_title(f'Event Flow Diagram for Device ID: {self.config.device_id}', 
                    fontsize=14, fontweight='bold')
        
        # Remove y-axis ticks
        ax.set_yticks([])
        
        # Set x-axis to show event numbers
        ax.set_xticks(x_positions)
        ax.set_xticklabels([f'E{i+1}' for i in range(num_events)])
        
        # Add legend
        legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.7, label=topic.replace('dp-', '').replace('-topic', ''))
                          for topic, color in topic_colors.items()]
        ax.legend(handles=legend_elements, bbox_to_anchor=(1.02, 1), loc='upper left')
        
        plt.tight_layout()
        
        # Save
        flow_path = os.path.join(self.config.output_dir, f"flow_{self.config.device_id}.png")
        plt.savefig(flow_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Event flow diagram saved to: {flow_path}")
        return flow_path
    
    def generate_compact_timeline(self) -> str:
        """Generate a compact timeline view - most convenient for event lines"""
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available, skipping compact timeline generation")
            return ""
        
        if not self.events:
            logger.warning("No events to plot")
            return ""
        
        logger.info("Generating compact timeline visualization")
        
        # Sort events by timestamp
        sorted_events = sorted(self.events, key=lambda x: x.timestamp)
        
        # Create figure with wider aspect ratio
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(22, 10), height_ratios=[3, 1])
        
        # Group by event type for color coding
        event_type_colors = {}
        colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
        
        unique_event_types = list(set(event.event_type for event in sorted_events))
        for i, event_type in enumerate(unique_event_types):
            event_type_colors[event_type] = colors[i % len(colors)]
        
        # Main timeline plot with non-linear mapping
        timestamps = [event.timestamp for event in sorted_events]
        mapped_timestamps, original_sorted = self._create_nonlinear_time_mapping(timestamps)
        
        y_positions = [0] * len(mapped_timestamps)
        
        # Plot events as points
        for i, (event, mapped_time) in enumerate(zip(sorted_events, mapped_timestamps)):
            color = event_type_colors[event.event_type]
            ax1.scatter(mapped_time, 0, c=color, s=120, alpha=0.8, 
                       edgecolors='black', linewidth=1, zorder=3)
            
            # Add event labels with smart positioning (showing original time)
            label_y = 0.1 if i % 2 == 0 else -0.1
            ax1.annotate(f'{event.event_type}\n{event.timestamp.strftime("%H:%M:%S")}', 
                        (mapped_time, label_y),
                        rotation=45, fontsize=8, ha='left', va='bottom' if label_y > 0 else 'top',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.6))
        
        # Connect events with lines
        if len(mapped_timestamps) > 1:
            ax1.plot(mapped_timestamps, y_positions, 'k-', alpha=0.3, linewidth=1, zorder=1)
        
        # Customize main plot
        ax1.set_ylim(-0.3, 0.3)
        ax1.set_title(f'Compact Event Timeline for Device ID: {self.config.device_id} (Non-linear scale)', 
                     fontsize=14, fontweight='bold')
        ax1.set_yticks([])
        ax1.grid(True, alpha=0.3, axis='x')
        ax1.set_xlabel('Time (Non-linear: compressed gaps, expanded clusters)', fontsize=10, fontweight='bold')
        
        # Second plot: Event type distribution
        event_type_counts = {}
        for event in sorted_events:
            event_type_counts[event.event_type] = event_type_counts.get(event.event_type, 0) + 1
        
        event_types = list(event_type_counts.keys())
        counts = list(event_type_counts.values())
        colors_for_bars = [event_type_colors[et] for et in event_types]
        
        bars = ax2.bar(event_types, counts, color=colors_for_bars, alpha=0.7, edgecolor='black')
        ax2.set_title('Event Type Distribution', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Count', fontsize=10)
        
        # Add count labels on bars
        for bar, count in zip(bars, counts):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                    str(count), ha='center', va='bottom', fontweight='bold')
        
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
        
        # Overall formatting
        plt.tight_layout()
        
        # Save
        compact_path = os.path.join(self.config.output_dir, f"compact_{self.config.device_id}.png")
        plt.savefig(compact_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Compact timeline saved to: {compact_path}")
        return compact_path
    
    def generate_html_report(self, graph_paths: dict) -> str:
        """Generate an interactive HTML report with all visualizations"""
        if not self.events:
            logger.warning("No events to generate HTML report")
            return ""
        
        logger.info("Generating interactive HTML report")
        
        # HTML template
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KomniTrace Report - Device {self.config.device_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}
        .stat-label {{
            color: #666;
            margin-top: 5px;
        }}
        .visualization-section {{
            background: white;
            margin-bottom: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .viz-header {{
            background: #667eea;
            color: white;
            padding: 15px;
            font-size: 1.2em;
            font-weight: bold;
        }}
        .viz-content {{
            padding: 20px;
            text-align: center;
        }}
        .viz-image {{
            max-width: 100%;
            height: auto;
            border-radius: 5px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }}
        .viz-description {{
            margin-top: 15px;
            color: #666;
            text-align: left;
        }}
        .event-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        .event-table th, .event-table td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        .event-table th {{
            background-color: #667eea;
            color: white;
        }}
        .event-table tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .tabs {{
            display: flex;
            background: #f0f0f0;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .tab {{
            flex: 1;
            padding: 10px;
            text-align: center;
            cursor: pointer;
            background: #f0f0f0;
            border: none;
            transition: background 0.3s;
        }}
        .tab.active {{
            background: #667eea;
            color: white;
        }}
        .tab-content {{
            display: none;
        }}
        .tab-content.active {{
            display: block;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 KomniTrace Event Analysis Report</h1>
            <h2>Device ID: {self.config.device_id}</h2>
            <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{len(self.events)}</div>
                <div class="stat-label">Total Events</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len(set(event.topic for event in self.events))}</div>
                <div class="stat-label">Unique Topics</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len(set(event.event_type for event in self.events))}</div>
                <div class="stat-label">Event Types</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{((max(event.timestamp for event in self.events) - min(event.timestamp for event in self.events)).total_seconds() / 3600):.1f}h</div>
                <div class="stat-label">Time Span</div>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab active" onclick="showTab('overview')">📊 Overview</button>
            <button class="tab" onclick="showTab('timeline')">⏱️ Timeline</button>
            <button class="tab" onclick="showTab('swimlane')">🏊 Swimlane</button>
            <button class="tab" onclick="showTab('flow')">🔄 Flow</button>
            <button class="tab" onclick="showTab('compact')">📏 Compact</button>
            <button class="tab" onclick="showTab('events')">📋 Events</button>
        </div>
        
        <div id="overview" class="tab-content active">
            <div class="visualization-section">
                <div class="viz-header">📈 Event Analysis Overview</div>
                <div class="viz-content">
                    <h3>Event Distribution by Topic</h3>
"""
        
        # Add event distribution analysis
        topic_counts = {}
        event_type_counts = {}
        for event in self.events:
            topic_counts[event.topic] = topic_counts.get(event.topic, 0) + 1
            event_type_counts[event.event_type] = event_type_counts.get(event.event_type, 0) + 1
        
        html_content += """
                    <table class="event-table">
                        <tr><th>Topic</th><th>Event Count</th><th>Percentage</th></tr>
"""
        
        for topic, count in topic_counts.items():
            percentage = (count / len(self.events)) * 100
            topic_short = topic.replace('dp-', '').replace('-topic', '')
            html_content += f"                        <tr><td>{topic_short}</td><td>{count}</td><td>{percentage:.1f}%</td></tr>\n"
        
        html_content += """
                    </table>
                    
                    <h3 style="margin-top: 30px;">Event Types Distribution</h3>
                    <table class="event-table">
                        <tr><th>Event Type</th><th>Count</th><th>Percentage</th></tr>
"""
        
        for event_type, count in event_type_counts.items():
            percentage = (count / len(self.events)) * 100
            html_content += f"                        <tr><td>{event_type}</td><td>{count}</td><td>{percentage:.1f}%</td></tr>\n"
        
        # Add visualization tabs
        visualizations = [
            ('timeline', 'Standard Timeline View', 'Nested event types within topics for comprehensive overview'),
            ('swimlane', 'Swimlane Timeline', 'Horizontal timeline ideal for sequential event analysis'),
            ('flow', 'Event Flow Diagram', 'Step-by-step event progression showing the complete sequence'),
            ('compact', 'Compact Timeline', 'Most convenient view with event distribution analysis')
        ]
        
        for viz_id, title, description in visualizations:
            if viz_id in graph_paths and graph_paths[viz_id]:
                # Get relative path for image
                img_name = os.path.basename(graph_paths[viz_id])
                html_content += f"""
        </table>
                </div>
            </div>
        </div>
        
        <div id="{viz_id}" class="tab-content">
            <div class="visualization-section">
                <div class="viz-header">📊 {title}</div>
                <div class="viz-content">
                    <img src="{img_name}" alt="{title}" class="viz-image">
                    <div class="viz-description">
                        <p><strong>Description:</strong> {description}</p>
                    </div>
                </div>
            </div>
        </div>
"""
        
        # Add events table
        html_content += """
        <div id="events" class="tab-content">
            <div class="visualization-section">
                <div class="viz-header">📋 Detailed Event List</div>
                <div class="viz-content">
                    <table class="event-table">
                        <tr>
                            <th>#</th>
                            <th>Timestamp</th>
                            <th>Topic</th>
                            <th>Event Type</th>
                            <th>Message Preview</th>
                        </tr>
"""
        
        for i, event in enumerate(sorted(self.events, key=lambda x: x.timestamp), 1):
            topic_short = event.topic.replace('dp-', '').replace('-topic', '')
            message_preview = event.message[:100] + "..." if len(event.message) > 100 else event.message
            timestamp_str = event.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            html_content += f"""
                        <tr>
                            <td>{i}</td>
                            <td>{timestamp_str}</td>
                            <td>{topic_short}</td>
                            <td>{event.event_type}</td>
                            <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis;">{message_preview}</td>
                        </tr>
"""
        
        html_content += """
                    </table>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        function showTab(tabName) {
            // Hide all tab contents
            const contents = document.querySelectorAll('.tab-content');
            contents.forEach(content => content.classList.remove('active'));
            
            // Remove active class from all tabs
            const tabs = document.querySelectorAll('.tab');
            tabs.forEach(tab => tab.classList.remove('active'));
            
            // Show selected tab content
            document.getElementById(tabName).classList.add('active');
            
            // Add active class to clicked tab
            event.target.classList.add('active');
        }
    </script>
</body>
</html>
"""
        
        # Save HTML report
        html_path = os.path.join(self.config.output_dir, f"report_{self.config.device_id}.html")
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Interactive HTML report saved to: {html_path}")
        return html_path
    
    def generate_plotly_interactive_timeline(self) -> str:
        """Generate an interactive timeline using Plotly - HIGHLY RECOMMENDED for dynamic control"""
        if not PLOTLY_AVAILABLE:
            logger.warning("plotly not available, skipping interactive timeline generation")
            logger.info("To enable interactive visualizations, install plotly: pip install plotly")
            return ""
        
        if not self.events:
            logger.warning("No events to plot")
            return ""
        
        logger.info("Generating interactive Plotly timeline")
        
        # Sort events by timestamp
        sorted_events = sorted(self.events, key=lambda x: x.timestamp)
        
        # Create subplots with secondary y-axis for different views
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('Timeline View', 'Event Type Distribution', 'Topic Activity'),
            vertical_spacing=0.08,
            specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
        )
        
        # Color mapping for consistency
        topic_colors = {
            'dp-provision-events-topic': '#3498db',
            'dp-notifications-topic': '#e74c3c', 
            'dp-provision-upstream-events-topic': '#2ecc71'
        }
        
        event_type_colors = {}
        colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
        unique_event_types = list(set(event.event_type for event in sorted_events))
        for i, event_type in enumerate(unique_event_types):
            event_type_colors[event_type] = colors[i % len(colors)]
        
        # 1. Main Timeline - Interactive scatter plot with zoom, pan, hover
        for event_type in unique_event_types:
            events_of_type = [e for e in sorted_events if e.event_type == event_type]
            
            timestamps = [e.timestamp for e in events_of_type]
            topics = [e.topic.replace('dp-', '').replace('-topic', '') for e in events_of_type]
            
            # Create hover text with full event details
            hover_text = []
            for event in events_of_type:
                hover_info = (
                    f"<b>Event Type:</b> {event.event_type}<br>"
                    f"<b>Topic:</b> {event.topic}<br>"
                    f"<b>Time:</b> {event.timestamp.strftime('%Y-%m-%d %H:%M:%S')}<br>"
                    f"<b>Message:</b> {event.message[:100]}..."
                )
                hover_text.append(hover_info)
            
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=topics,
                    mode='markers',
                    marker=dict(
                        size=12,
                        color=event_type_colors[event_type],
                        line=dict(width=2, color='darkblue')
                    ),
                    name=f'{event_type} ({len(events_of_type)})',
                    text=hover_text,
                    hovertemplate='%{text}<extra></extra>',
                    showlegend=True
                ),
                row=1, col=1
            )
        
        # 2. Event Type Distribution - Bar chart of event counts by type
        event_type_counts = {et: 0 for et in unique_event_types}
        for event in sorted_events:
            event_type_counts[event.event_type] += 1
        
        fig.add_trace(
            go.Bar(
                x=list(event_type_counts.keys()),
                y=list(event_type_counts.values()),
                marker_color=[event_type_colors[et] for et in event_type_counts.keys()],
                text=list(event_type_counts.values()),
                textposition='auto',
                name='Event Type Counts'
            ),
            row=2, col=1
        )
        
        # 3. Topic Activity - Heatmap of event density by topic and time
        if PLOTLY_AVAILABLE:
            try:
                from plotly.colors import diverging
                
                # Prepare data for heatmap: count events per topic per hour
                topic_activity = defaultdict(lambda: defaultdict(int))
                for event in sorted_events:
                    topic_short = event.topic.replace('dp-', '').replace('-topic', '')
                    hour = event.timestamp.strftime('%Y-%m-%d %H:%M')
                    topic_activity[topic_short][hour] += 1
                
                # Convert to matrix form for heatmap: topics x time slots
                topics = list(topic_activity.keys())
                time_slots = sorted({hour for hours in topic_activity.values() for hour in hours})
                activity_matrix = []
                
                for topic in topics:
                    row = [topic_activity[topic].get(slot, 0) for slot in time_slots]
                    activity_matrix.append(row)
                
                # Define color scale
                if len(topics) > 0:
                    colorscale = 'Viridis'  # Use a safe colorscale
                    
                    fig.add_trace(
                        go.Heatmap(
                            z=activity_matrix,
                            x=time_slots,
                            y=topics,
                            colorscale=colorscale,
                            colorbar=dict(title='Event Count'),
                            hovertemplate='Topic: %{y}<br>Time: %{x}<br>Count: %{z}<extra></extra>'
                        ),
                        row=3, col=1
                    )
            except Exception as e:
                logger.warning(f"Could not create heatmap: {e}")
        
        # Update layout for better spacing and titles
        fig.update_layout(
            height=800,
            title_text=f"Interactive Event Analysis for Device ID: {self.config.device_id}",
            title_x=0.5,
            xaxis_title="Time",
            yaxis_title="Topics / Event Types",
            showlegend=True
        )
        
        # Save the interactive plot as HTML file
        plotly_path = os.path.join(self.config.output_dir, f"interactive_timeline_{self.config.device_id}.html")
        fig.write_html(plotly_path)
        
        logger.info(f"Interactive Plotly timeline saved to: {plotly_path}")
        return plotly_path
    
    def save_events_to_json(self) -> str:
        """Save events to a JSON file"""
        events_file = os.path.join(self.config.output_dir, f"events_{self.config.device_id}.json")
        with open(events_file, 'w') as f:
            events_data = []
            for event in self.events:
                event_dict = {
                    'timestamp': event.timestamp.isoformat(),
                    'topic': event.topic,
                    'device_id': event.device_id,
                    'event_type': event.event_type,
                    'message': event.message,
                    'raw_data': event.raw_data,
                    'partition': event.partition,
                    'offset': event.offset
                }
                events_data.append(event_dict)
            json.dump(events_data, f, indent=2)
        
        logger.info(f"Events saved to: {events_file}")
        return events_file
