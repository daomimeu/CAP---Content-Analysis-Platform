import streamlit as st

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from datetime import datetime
from io import BytesIO

from PIL import Image
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account


st.set_page_config(layout='wide', page_title='CAP - Home')

credentials = service_account.Credentials.from_service_account_file('xxx-6d563dad79b2.json')
storage_client = storage.Client(project='xxx', credentials=credentials)
bq_client = bigquery.Client(project='xxx', credentials=credentials)


st.markdown("# Homepage")
st.write(
    """Welcome to content analytics platform, your one-stop shop for comprehensive campaign creative performance analysis.
    If this is your first time here, read below for the intended usage of each of our analysis tools."""
)

# Splitting into three columns
col1, col2, col3 = st.columns(3)

# Campaign Content Analysis Section
with col1:
    with st.expander("📊 Campaign Content Analysis"):
        st.markdown(
            """
            **Gain comprehensive insights into CRM campaign performance, benchmarked against similar campaigns.**
            Identify strengths and areas for improvement to enhance future campaign success.
            """
        )
        st.subheader("Metrics Measured:")
        st.write(
            """
            - **EDM Open Rates**  
            - **EDM Subject Line AI-assisted Analysis**  
            - **EDM Click Through Rates**  
            - **EDM Click Contribution by Pod Height**  
            - **PN Engagement Rates**  
            - **PN Ticker AI-assisted Analysis**  
            - **PN Text-in-image Character Count**  
            - **PN Text-to-image Ratio**  
            """
        )

# Content Comparison Section
with col2:
    with st.expander("🔍 Content Comparison"):
        st.markdown(
            """
            **Easily compare the performance of campaigns based on your criteria.**
            """
        )
        st.write(
            """
            - Analyze campaigns targeted at different audience segments within the same wave  
            - Evaluate the consistency of impact across waves for the same segment  
            - Assess messaging effectiveness across markets  
            """
        )
        st.info("💡 Prepare your list of Campaign IDs to uncover actionable insights.")

# Subject Line Best Practices Section
with col3:
    with st.expander("✍️ Subject Line Best Practices"):
        st.markdown(
            """
            **Explore the secrets behind high-performing subject lines.**  
            """
        )
        st.write(
            """
            - Understand how top-performing subject lines are structured with AI-driven analysis.  
            - Review examples of the best-performing subject lines for inspiration.  
            - Compare commonly used words in top-performing subject lines with those in average ones to identify patterns and opportunities.  
            """
        )


# Version Details and Roadmap Section
st.markdown("---")  # Separator line for better visual separation
with st.expander("📅 Version Details and Roadmap"):
    st.subheader("Current Limitations (12-12-2024):")
    st.write(
        """
        - Subject Line Data available from 01-01-2023 till 31-10-2024.
        - EDM Visual Data available from 01-11-2024 till 30-11-2024.
        - Push Notification Analysis currently WIP
        """
    )
    st.subheader("Upcoming Features (31-12-2024):")
    st.write(
        """
        - **1:** Integration of Push Notification Analysis (Campaigns between 01-01-2023 till 30-11-2024)
        - **2:** Data population of EDM Content Visuals (Campaigns between 01-01-2024 till 30-11-2024)
        """
    )
    st.info("💡 Stay tuned for updates and feel free to share feedback to help us improve!")