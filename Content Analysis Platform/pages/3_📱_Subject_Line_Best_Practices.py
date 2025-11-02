import streamlit as st
from streamlit_extras.stylable_container import stylable_container

import pandas as pd
import numpy as np

from google.cloud import bigquery
from google.oauth2 import service_account

import core.sl_utils as sl
import core.chart_utils as ch

import plotly.graph_objects as go
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from random import sample



st.set_page_config(layout='wide', page_title='CAP - Subject Line BP')

credentials = service_account.Credentials.from_service_account_file('xxx-6d563dad79b2.json')
bq_client = bigquery.Client(project='xxx', credentials=credentials)


with st.sidebar:
    st.header("Samsung SEAO Content Analytics")
    
    st.write("EMAIL")
    form_edm = st.form(key='form_edm')
    market = form_edm.selectbox('Market',('SG', 'ID', 'MY', 'NZ', 'PH', 'VN'))
    objective = form_edm.selectbox('Objective', ('Awareness', 'Conversion (PO)', 'Conversion (Launch)', 'Conversion (Sustain)','Engagement'))
    product = form_edm.selectbox('Product',('MX', 'CE'))
    submit_form_edm = form_edm.form_submit_button(label='Apply Filters')

    st.write("PUSH")
    form_pn= st.form(key='form_pn')
    market = form_pn.selectbox('Market',('SG', 'ID', 'MY', 'NZ', 'PH', 'VN'))
    submit_form_pn = form_pn.form_submit_button(label='Apply Filters')


def get_cutes_score(market, objective, product):
    # Set up job configuration with parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market),
            bigquery.ScalarQueryParameter("objective", "STRING", objective),
            bigquery.ScalarQueryParameter("product", "STRING", product)
        ]
    )

    # Define the query
    QUERY_CUTES = """
        WITH bp_campaigns AS (
            SELECT
                curiosity, urgency, tone, emotion, specificity
            FROM
                content.bp_edm_sl
            WHERE
                country = @market
                AND product = @product
                AND objective = @objective
                AND top_flag = '1'
        ),

        other_campaigns AS (
            SELECT
                curiosity, urgency, tone, emotion, specificity
            FROM
                content.bp_edm_sl
            WHERE
                country = @market
                AND product = @product
                AND objective = @objective
                AND top_flag = '0'
        ),

        unpivoted_bp_campaigns AS (
            SELECT
                variable,
                value
            FROM
                bp_campaigns
            UNPIVOT (
                value FOR variable IN (
                    curiosity, urgency, tone, emotion, specificity
                )
            )
        ),

        unpivoted_other_campaigns AS (
            SELECT
                variable,
                value
            FROM
                other_campaigns
            UNPIVOT (
                value FOR variable IN (
                    curiosity, urgency, tone, emotion, specificity
                )
            )
        )

        SELECT
            bp.variable AS Approach,
            bp.value AS `Best Performing`,
            oth.value AS `All Campaigns`,
            bp.value - oth.value AS Difference
        FROM
            unpivoted_bp_campaigns bp
        JOIN
            unpivoted_other_campaigns oth
            ON bp.variable = oth.variable
    """

    # Run the query and convert to dataframe
    df_cutes = bq_client.query(QUERY_CUTES, job_config=job_config).to_dataframe()
    
    # Return the dataframe with the results
    return df_cutes

def get_binary_var_bp(market, objective, product):
    """
    Fetch and preprocess the binary variable Best Practice data from BigQuery.

    Parameters:
        market (str): Market name.
        objective (str): Objective name.
        product (str): Product name.

    Returns:
        pd.DataFrame: Processed DataFrame with 'Rank', 'Features', 'Importance_Stars', and 'Recommendation' columns.
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market),
            bigquery.ScalarQueryParameter("objective", "STRING", objective),
            bigquery.ScalarQueryParameter("product", "STRING", product)
        ]
    )

    QUERY_BP_BV = """
        WITH magnitude_data AS (
            SELECT
                emoji,personalization,offer,product_name,feature,question,exclamation,ai, length_long, length_med, length_short
            FROM
                content.bp_edm_sl
            WHERE
                country = @market
                AND product = @product
                AND objective = @objective
                AND top_flag = 'magnitude'
            ),
        direction_data AS (
            SELECT
                emoji,personalization,offer,product_name,feature,question,exclamation,ai, length_long, length_med, length_short
            FROM
                content.bp_edm_sl
            WHERE
                country = @market
                AND product = @product
                AND objective = @objective
                AND top_flag = 'direction'
            ),
        unpivoted_magnitude AS (
            SELECT
                'magnitude' AS top_flag,
                variable,
                value
            FROM
                magnitude_data
            UNPIVOT (
                value FOR variable IN (
                emoji, personalization, offer, product_name, feature, 
                question, exclamation, ai, length_long, length_med, length_short
                )
            )
            ),
        unpivoted_direction AS (
            SELECT
                'direction' AS top_flag,
                variable,
                value
            FROM
                direction_data
            UNPIVOT (
                value FOR variable IN (
                emoji, personalization, offer, product_name, feature, 
                question, exclamation, ai, length_long, length_med, length_short
                )
            )
            )
        SELECT
            m.variable AS Features,
            ABS(m.value) AS Importance,
            CASE WHEN d.value < 0 THEN 'Exclude' ELSE 'Include' END AS Recommendation
        FROM
            unpivoted_magnitude m
        JOIN
            unpivoted_direction d
        ON m.variable = d.variable
        ORDER BY 2 DESC
    """

    # Fetch the data from BigQuery
    df_bv_bp = bq_client.query(QUERY_BP_BV, job_config=job_config).to_dataframe()

    # Preprocess the DataFrame
    def importance_to_stars(value, max_value=1.0, char="*"):
        max_stars = 20  # Total number of stars for max progress
        num_stars = int((value / max_value) * max_stars)
        return char * num_stars
    
    # Filter for rows where Recommendation is 'Include'
    df_bv_bp = df_bv_bp[df_bv_bp["Recommendation"] == "Include"].reset_index(drop=True)

    # Add a "Rank" column starting from 1
    df_bv_bp.insert(0, "Rank", range(1, len(df_bv_bp) + 1))

    # Add a new column with star-based progress representation
    df_bv_bp["Importance_Stars"] = df_bv_bp["Importance"].apply(importance_to_stars)

    return df_bv_bp

def best_sl(market, objective, product):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market),
            bigquery.ScalarQueryParameter("objective", "STRING", objective),
            bigquery.ScalarQueryParameter("product", "STRING", product)
        ]
    )

    QUERY_BEST_SL = """
        SELECT subject_line, rank, country, product, objective
        FROM xxx.content.bp_edm_sl_perf
        WHERE top_flag=1 AND country = @market AND product = @product AND objective = @objective
    """

    df_best_sl = bq_client.query(QUERY_BEST_SL, job_config=job_config).to_dataframe()
    return df_best_sl

def other_sl(market, objective, product):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market),
            bigquery.ScalarQueryParameter("objective", "STRING", objective),
            bigquery.ScalarQueryParameter("product", "STRING", product)
        ]
    )

    QUERY_BEST_SL = """
        SELECT subject_line, rank
        FROM xxx.content.bp_edm_sl_perf
        WHERE top_flag=0 AND country = @market AND product = @product AND objective = @objective
    """

    df_other_sl = bq_client.query(QUERY_BEST_SL, job_config=job_config).to_dataframe()
    return df_other_sl    


if submit_form_edm:

    st.header('EMAIL Subject Line Best Practices')
    # Data required for first container
    df_bv_bp = get_binary_var_bp(market, objective, product)
    df_cutes = get_cutes_score(market, objective, product)  
     
    # First container with analysis
    with stylable_container(
        key="analysis_container", 
        css_styles="""{
            background-color: white; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}"""
    ):

        # Create left and right columns for display
        col_ratios_sl = [0.6, 0.4]  # C.U.T.E.S and features
        cols_sl = st.columns(col_ratios_sl, gap="medium")

        # Left column: C.U.T.E.S Analysis
        cols_sl[0].subheader("C.U.T.E.S Analysis")

        # cols_sl[0].markdown("**C.U.T.E.S score (tagged by Gemini)**")
        cutes_def = cols_sl[0].popover("C.U.T.E.S score (tagged by Gemini)")
        cutes_def.markdown(f'''
            *:blue[Curiosity]: Informative subject lines directly state a fact, while curiosity-inducing ones leave things unsaid, sparking interest and prompting further investigation.  
            *:blue[Urgency]: Continuous subject lines convey no sense of urgency, while those with a limited timeframe encourage immediate attention and action.  
            *:blue[Tone]: Formal subject lines are professional and distant, while casual ones are informal and friendly, making the communication feel more approachable.  
            *:blue[Emotion]: Subject lines with negative emotion focus on avoiding problems, while those with positive emotion emphasize benefits and aspirations, creating a more uplifting tone.  
            *:blue[Specificity]: Generic subject lines apply broadly to all, while personalized ones are tailored to specific interests, behaviors, or traits of the audience.  

            <br><br>
            <span style="color: #AA5486;">&#8226;</span> Best Practice Average
            <br>
            <span style="color: #9ABF80;">&#8226;</span> All Campaigns Average
        ''', unsafe_allow_html=True)

        # Instantiate CUTES score dataframes
        best_practice_scores = df_cutes['Best Performing'].tolist()
        average_scores = df_cutes['All Campaigns'].tolist()

        # Plot CUTES chart
        fig, config = ch.make_cutes_chart(chart_height=200, y1_data=average_scores, y2_data=best_practice_scores)
        cols_sl[0].plotly_chart(fig, use_container_width=True, config=config)

        # Left column, below C.U.T.E.S Analysis: Recommendations
        top3_recommendations = sl.get_top3_recommendations(df_cutes)
        recommendation_str = "\n".join(top3_recommendations)
        cols_sl[0].markdown('<span style="color: green;">**Recommendations**:</span>', unsafe_allow_html=True)
        cols_sl[0].text(recommendation_str)

        # Right column: Feature Analysis
        cols_sl[1].subheader("Feature Analysis")

        # Function to convert `Importance` into progress bar string
        def importance_to_stars(value, max_value=1.0, char="*"):
            max_stars = 20  # Total number of stars for max progress
            num_stars = int((value / max_value) * max_stars)
            return char * num_stars

        # Add a new column with star-based progress representation
        df_bv_bp["Importance_Stars"] = df_bv_bp["Importance"].apply(importance_to_stars)

        cols_sl[1].data_editor(
            df_bv_bp[["Rank", "Features", "Importance_Stars", "Recommendation"]], 
            column_config={
                "Rank": st.column_config.NumberColumn(
                    label="Rank",
                    help="Feature ranking based on importance",
                    format="%d"
                ),
                "Importance_Stars": st.column_config.TextColumn(
                    label="Importance", 
                    help="Star representation of feature importance (max 20 stars)"
                ),
                "Features": st.column_config.TextColumn(
                    label="Features",
                    help="Feature names analyzed for importance"
                ),
                "Recommendation": st.column_config.TextColumn(
                    label="Recommendation",
                    help="Action to take for each feature"
                )
            },
            hide_index=True
        )

    # Second container for top 10 subject lines

    with stylable_container(
        key="sl_container", 
        css_styles="""{
            background-color: white; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}"""
    ):
        st.subheader("Top 10 Best Performing Subject Lines")

        # Fetch the best performing subject lines (assume df_best_sl is already fetched)
        df_best_sl = best_sl(market, objective, product)
        
        # Display the top 10 subject lines sorted by rank
        top_10_sl = df_best_sl.sort_values('rank').head(10)['subject_line'].to_list()

        # Display the subject lines
        for t in top_10_sl:  # Always display only 10 lines
            st.markdown(t)

    # Third container for wordclouds
    with stylable_container(
        key="wordcloud_container", 
        css_styles="""{
            background-color: white; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}""",
        ):
        # Fetch the best performing subject lines and concatenate into one string
        df_best_sl = best_sl(market, objective, product)  
        bp_text = " ".join(df_best_sl['subject_line'].dropna())         
        # Fetch the other subject lines and concatenate into one string
        df_other_sl = other_sl(market, objective, product)  
        oth_text = " ".join(df_other_sl['subject_line'].dropna())

        # Generate the circular word clouds
        wc_best = ch.generate_circular_wordcloud(bp_text)
        wc_others = ch.generate_circular_wordcloud(oth_text)

        # Display in two columns
        col3, col4, = st.columns(2)
        with col3:
            st.subheader("Words used in most engaged subject lines")
            st.pyplot(wc_best)
        with col4:
            st.subheader("Words used in other subject lines")
            st.pyplot(wc_others)


### PUSH

def get_cutes_score_pn(market):
    # Set up job configuration with parameters
    job_config = bigquery.QueryJobConfig(
            query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market)
        ]
    )

    # Define the query
    QUERY_CUTES = """
        WITH bp_campaigns AS (
            SELECT
                curiosity, urgency, tone, emotion, specificity
            FROM
                content.bp_pn_sl
            WHERE
                country = @market
                AND top_flag = '1'
        ),

        other_campaigns AS (
            SELECT
                curiosity, urgency, tone, emotion, specificity
            FROM
                content.bp_pn_sl
            WHERE
                country = @market
                AND top_flag = '0'
        ),

        unpivoted_bp_campaigns AS (
            SELECT
                variable,
                value
            FROM
                bp_campaigns
            UNPIVOT (
                value FOR variable IN (
                    curiosity, urgency, tone, emotion, specificity
                )
            )
        ),

        unpivoted_other_campaigns AS (
            SELECT
                variable,
                value
            FROM
                other_campaigns
            UNPIVOT (
                value FOR variable IN (
                    curiosity, urgency, tone, emotion, specificity
                )
            )
        )

        SELECT
            bp.variable AS Approach,
            bp.value AS `Best Performing`,
            oth.value AS `All Campaigns`,
            bp.value - oth.value AS Difference
        FROM
            unpivoted_bp_campaigns bp
        JOIN
            unpivoted_other_campaigns oth
            ON bp.variable = oth.variable
    """

    # Run the query and convert to dataframe
    df_cutes = bq_client.query(QUERY_CUTES, job_config=job_config).to_dataframe()
    
    # Return the dataframe with the results
    return df_cutes

def get_binary_var_bp_pn(market):
    """
    Fetch and preprocess the binary variable Best Practice data from BigQuery.

    Parameters:
        market (str): Market name.

    Returns:
        pd.DataFrame: Processed DataFrame with 'Rank', 'Features', 'Importance_Stars', and 'Recommendation' columns.
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market)
        ]
    )
    QUERY_BP_BV = """
        WITH magnitude_data AS (
            SELECT
                emoji,personalization,offer,product_name,feature,question,exclamation,ai, length_long, length_med, length_short
            FROM
                content.bp_pn_sl
            WHERE
                country = @market
                AND top_flag = 'magnitude'
            ),
        direction_data AS (
            SELECT
                emoji,personalization,offer,product_name,feature,question,exclamation,ai, length_long, length_med, length_short
            FROM
                content.bp_pn_sl
            WHERE
                country = @market
                AND top_flag = 'direction'
            ),
        unpivoted_magnitude AS (
            SELECT
                'magnitude' AS top_flag,
                variable,
                value
            FROM
                magnitude_data
            UNPIVOT (
                value FOR variable IN (
                emoji, personalization, offer, product_name, feature, 
                question, exclamation, ai, length_long, length_med, length_short
                )
            )
            ),
        unpivoted_direction AS (
            SELECT
                'direction' AS top_flag,
                variable,
                value
            FROM
                direction_data
            UNPIVOT (
                value FOR variable IN (
                emoji, personalization, offer, product_name, feature, 
                question, exclamation, ai, length_long, length_med, length_short
                )
            )
            )
        SELECT
            m.variable AS Features,
            ABS(m.value) AS Importance,
            CASE WHEN d.value < 0 THEN 'Exclude' ELSE 'Include' END AS Recommendation
        FROM
            unpivoted_magnitude m
        JOIN
            unpivoted_direction d
        ON m.variable = d.variable
        ORDER BY 2 DESC
    """

    # Fetch the data from BigQuery
    df_bv_bp = bq_client.query(QUERY_BP_BV, job_config=job_config).to_dataframe()

    # Preprocess the DataFrame
    def importance_to_stars(value, max_value=1.0, char="*"):
        max_stars = 20  # Total number of stars for max progress
        num_stars = int((value / max_value) * max_stars)
        return char * num_stars
    
    # Filter for rows where Recommendation is 'Include'
    df_bv_bp = df_bv_bp[df_bv_bp["Recommendation"] == "Include"].reset_index(drop=True)

    # Add a "Rank" column starting from 1
    df_bv_bp.insert(0, "Rank", range(1, len(df_bv_bp) + 1))

    # Add a new column with star-based progress representation
    df_bv_bp["Importance_Stars"] = df_bv_bp["Importance"].apply(importance_to_stars)

    return df_bv_bp

def best_sl_pn(market):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market)
        ]
    )

    QUERY_BEST_SL = """
        SELECT subject_line, rank, country
        FROM xxx.content.bp_pn_sl_perf
        WHERE top_flag=1 AND country = @market
    """

    df_best_sl = bq_client.query(QUERY_BEST_SL, job_config=job_config).to_dataframe()
    return df_best_sl

def other_sl_pn(market):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("market", "STRING", market)
        ]
    )
    QUERY_BEST_SL = """
        SELECT subject_line, rank
        FROM xxx.content.bp_pn_sl_perf
        WHERE top_flag=0 AND country = @market 
    """

    df_other_sl = bq_client.query(QUERY_BEST_SL, job_config=job_config).to_dataframe()
    return df_other_sl    


if submit_form_pn:
    st.header('PUSH Title Best Practices')

    # Data required for first container
    df_bv_bp = get_binary_var_bp_pn(market)
    df_cutes = get_cutes_score_pn(market)  
     
    # First container with analysis
    with stylable_container(
        key="analysis_container", 
        css_styles="""{
            background-color: white; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}"""
    ):

        # Create left and right columns for display
        col_ratios_sl = [0.6, 0.4]  # C.U.T.E.S and features
        cols_sl = st.columns(col_ratios_sl, gap="medium")

        # Left column: C.U.T.E.S Analysis
        cols_sl[0].subheader("C.U.T.E.S Analysis")

        # cols_sl[0].markdown("**C.U.T.E.S score (tagged by Gemini)**")
        cutes_def = cols_sl[0].popover("C.U.T.E.S score (tagged by Gemini)")
        cutes_def.markdown(f'''
            *:blue[Curiosity]: Informative subject lines directly state a fact, while curiosity-inducing ones leave things unsaid, sparking interest and prompting further investigation.  
            *:blue[Urgency]: Continuous subject lines convey no sense of urgency, while those with a limited timeframe encourage immediate attention and action.  
            *:blue[Tone]: Formal subject lines are professional and distant, while casual ones are informal and friendly, making the communication feel more approachable.  
            *:blue[Emotion]: Subject lines with negative emotion focus on avoiding problems, while those with positive emotion emphasize benefits and aspirations, creating a more uplifting tone.  
            *:blue[Specificity]: Generic subject lines apply broadly to all, while personalized ones are tailored to specific interests, behaviors, or traits of the audience.  

            <br><br>
            <span style="color: #AA5486;">&#8226;</span> Best Practice Average
            <br>
            <span style="color: #9ABF80;">&#8226;</span> All Campaigns Average
        ''', unsafe_allow_html=True)

        # Instantiate CUTES score dataframes
        best_practice_scores = df_cutes['Best Performing'].tolist()
        average_scores = df_cutes['All Campaigns'].tolist()

        # Plot CUTES chart
        fig, config = ch.make_cutes_chart(chart_height=200, y1_data=average_scores, y2_data=best_practice_scores)
        cols_sl[0].plotly_chart(fig, use_container_width=True, config=config)

        # Left column, below C.U.T.E.S Analysis: Recommendations
        top3_recommendations = sl.get_top3_recommendations(df_cutes)
        recommendation_str = "\n".join(top3_recommendations)
        cols_sl[0].markdown('<span style="color: green;">**Recommendations**:</span>', unsafe_allow_html=True)
        cols_sl[0].text(recommendation_str)

        # Right column: Feature Analysis
        cols_sl[1].subheader("Feature Analysis")

        # Function to convert `Importance` into progress bar string
        def importance_to_stars(value, max_value=1.0, char="*"):
            max_stars = 20  # Total number of stars for max progress
            num_stars = int((value / max_value) * max_stars)
            return char * num_stars

        # Add a new column with star-based progress representation
        df_bv_bp["Importance_Stars"] = df_bv_bp["Importance"].apply(importance_to_stars)

        cols_sl[1].data_editor(
            df_bv_bp[["Rank", "Features", "Importance_Stars", "Recommendation"]], 
            column_config={
                "Rank": st.column_config.NumberColumn(
                    label="Rank",
                    help="Feature ranking based on importance",
                    format="%d"
                ),
                "Importance_Stars": st.column_config.TextColumn(
                    label="Importance", 
                    help="Star representation of feature importance (max 20 stars)"
                ),
                "Features": st.column_config.TextColumn(
                    label="Features",
                    help="Feature names analyzed for importance"
                ),
                "Recommendation": st.column_config.TextColumn(
                    label="Recommendation",
                    help="Action to take for each feature"
                )
            },
            hide_index=True
        )

    # Second container for top 10 subject lines

    with stylable_container(
        key="sl_container", 
        css_styles="""{
            background-color: white; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}"""
    ):
        st.subheader("Top 10 Best Performing Push Titles")

        # Fetch the best performing subject lines (assume df_best_sl is already fetched)
        df_best_sl = best_sl_pn(market)
        
        # Display the top 10 subject lines sorted by rank
        top_10_sl = df_best_sl.sort_values('rank').head(10)['subject_line'].to_list()

        # Display the subject lines
        for t in top_10_sl:  # Always display only 10 lines
            st.markdown(t)

    # Third container for wordclouds
    with stylable_container(
        key="wordcloud_container", 
        css_styles="""{
            background-color: white; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}""",
        ):
        # Fetch the best performing subject lines and concatenate into one string
        df_best_sl = best_sl_pn(market)  
        bp_text = " ".join(df_best_sl['subject_line'].dropna())         
        # Fetch the other subject lines and concatenate into one string
        df_other_sl = other_sl_pn(market)  
        oth_text = " ".join(df_other_sl['subject_line'].dropna())

        # Generate the circular word clouds
        wc_best = ch.generate_circular_wordcloud(bp_text)
        wc_others = ch.generate_circular_wordcloud(oth_text)

        # Display in two columns
        col3, col4, = st.columns(2)
        with col3:
            st.subheader("Words used in most engaged Push Titles")
            st.pyplot(wc_best)
        with col4:
            st.subheader("Words used in other Push Titles")
            st.pyplot(wc_others)
