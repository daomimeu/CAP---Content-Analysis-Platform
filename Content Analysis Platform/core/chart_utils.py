import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud

import plotly.graph_objects as go
from plotly.subplots import make_subplots


def make_cutes_chart(chart_height, y1_data, y2_data=[0, 0, 0, 0, 0], cutes_label_color='#22177A', y1_marker={'color':'#AA5486', 'opacity':1}, y2_marker={'color':'#9ABF80', 'opacity':1}):
    """
    Creates a custom chart comparing two sets of data using scatter plots with dual y-axes.

    Parameters:
    - chart_height (int): The height of the chart in pixels.
    - y1_data (list): Data for the first scatter plot (secondary_y=False).
    - y2_data (list): Data for the second scatter plot (secondary_y=True). Defaults to [0, 0, 0, 0, 0].
    - cutes_label_color (str): Color for the y-axis labels.
    - y1_marker (dict): Marker style for the first scatter plot. Defaults to magenta color with full opacity.
    - y2_marker (dict): Marker style for the second scatter plot. Defaults to green color with full opacity.

    Returns:
    - fig (plotly.graph_objects.Figure): A Plotly figure object with the configured chart.
    - config (dict): Configuration for rendering the chart, e.g., static display mode.
    """
    # Create a figure with secondary y-axes to allow for dual y-axis plots.
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add the first scatter plot. Data is reversed for plotting bottom-to-top alignment.
    fig.add_trace(
        go.Scatter(
            x=list(reversed(y1_data)), # Reverse the data order for correct alignment.
            y=['Generic', 'Negative', 'Formal', 'Continuous', 'Informative'], # Labels for the y-axis (primary).
            mode='markers', # Plot markers (no connecting lines).
            marker=y1_marker), # Style of the markers (color and opacity).
        secondary_y=False # Plot on the primary y-axis.
    )

    # Add the second scatter plot. Data is reversed and aligned with the secondary y-axis.
    fig.add_trace(
        go.Scatter(
            x=list(reversed(y2_data)), 
            y=['Exclusive', 'Positive', 'Casual', 'Time-bound', 'Curious'], 
            mode='markers', 
            marker=y2_marker),
        secondary_y=True
    )

    # Configure x-axis settings: Range 0.5 to 5.5 to give 0.5 margin outside of 1-5 scale, make x-axis ticks and labels invisible
    fig.update_xaxes(range=[0.5, 5.5], visible=False)

    fig.update_layout(yaxis_title=None, # No title for the primary y-axis.
                      margin=dict(l=0, r=0, t=0, b=0), # Set margins to remove extra whitespace.
                      height=chart_height, # Set chart height as per the parameter.
                      yaxis=dict(tickfont=dict(size=15, color=cutes_label_color)), # Customize primary y-axis labels.
                      yaxis2=dict(tickfont=dict(size=15, color=cutes_label_color)) # Customize secondary y-axis labels.
                      ) 
                        #Streamlit will takeover font theme, can't set global font preference https://plotly.com/python/multiple-axes/

    # Disable the legend for this chart.    
    fig.update(layout_showlegend=False)

    # - 'staticPlot': True makes the plot non-interactive, suitable for static display.
    config = {'staticPlot': True}
    
    return fig, config


def make_click_rate_chart(groups):
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Click Rate by Pod Position',  'Click Rate by Pod Relative Size (%)'))

    fig.add_trace(
        go.Bar(x=groups['position']['position'], y=groups['position']['click_rate'], marker_color='#0A5EB0', name='Click Rate'),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=groups['position']['position'], y=groups['position']['bm_click_rate'], marker_color='#9ABF80', name='Benchmark'),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(x=groups['height_bin']['height_bin'], y=groups['height_bin']['click_rate'], marker_color='#0A5EB0', showlegend = False),
        row=1, col=2
    )
    fig.add_trace(
        go.Bar(x=groups['height_bin']['height_bin'], y=groups['height_bin']['bm_click_rate'], marker_color='#9ABF80', showlegend = False),
        row=1, col=2
    )

    fig.update_layout(barmode='group', width=650)

    return fig

def generate_circular_wordcloud(text, mask_radius=130, width=500, height=500, background_color="white"):
    """
    Generates a circular word cloud from the input text.
    
    Parameters:
    - text (str): The text to generate the word cloud from.
    - mask_radius (int): The radius of the circular mask.
    - width (int): The width of the word cloud image.
    - height (int): The height of the word cloud image.
    - background_color (str): The background color of the word cloud image.

    Returns:
    - fig: The matplotlib figure containing the word cloud.
    """
    # Circle mask
    x, y = np.ogrid[:height, :width]
    mask = (x - width // 2) ** 2 + (y - height // 2) ** 2 > mask_radius ** 2
    mask = 255 * mask.astype(int)

    # Create the WordCloud object
    wc = WordCloud(height=height, width=width, background_color=background_color, mask=mask, repeat=True)
    wc.generate(text)

    # Create the plot
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")

    return fig