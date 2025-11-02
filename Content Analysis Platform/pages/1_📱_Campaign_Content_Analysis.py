import streamlit as st

import pandas as pd
import numpy as np

from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

import core.img_utils as im
import core.cp_utils as cp
import core.sl_utils as sl
import core.chart_utils as ch


st.set_page_config(layout='wide', page_title='CAP - Content Analysis')

credentials = service_account.Credentials.from_service_account_file('xxx.json') # Uses service account JSON file that contains our credentials
bq_client = bigquery.Client(project='xxx', credentials=credentials)
storage_client = storage.Client(project='xxx', credentials=credentials)
edm_bucket = 'creative-edm'

# Page Setup: Initializes app sidebar (left) appearance and settings
with st.sidebar:
    st.header("SEAO Content Analytics")

    form_campaign_id = st.form(key='campaign_id')
    campaign_id = form_campaign_id.text_input("Enter campaign ID", placeholder='0000111111, 0000222222') # Allows users to 
    campaign_obj = form_campaign_id.selectbox('Campaign objective',('Awareness', 'Conversion (PO)', 'Conversion (Launch)', 'Conversion (Sustain)', 'Engagement'))
    # reference = form_campaign_id.selectbox('Reference',('Q6B6 Best Practices - Pre Order', 'Q6B6 Best Practices - Teaser'))
    submit_campaign_id = form_campaign_id.form_submit_button(label='Analyze')


def get_campaign_data(campaign_id):
    """
    Fetches and processes campaign and click-level data for a given set of campaign IDs.

    Args:
        campaign_id (str or list): A single campaign ID or list of IDs to query.

    Returns:
        tuple: A tuple containing:
            - df (pd.DataFrame): Processed campaign-level DataFrame.
            - df_click (pd.DataFrame): Processed click-level DataFrame.
            - first_campaign (str): The first campaign ID in the result.
            - first_campaign_data (dict): Data dictionary for the first campaign.
            - not_found (list): List of campaign IDs that were not found in the query results.

    Raises:
        ValueError: If both resulting DataFrames are empty.
    """
    # Helper module to process 'campaign_id' input to be formatted (refer to core.cp_utils.py). Returns a list of campaign_ids
    campaign_list = cp.parse_campaign_id(campaign_id)

    # Injects list of campaign_ids into query as a parameter
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("campaign_list", "STRING", campaign_list) # Parameters: ("Placeholder name of SQL query", "Data type of array", Value i.e. list of campaign_ids)
        ]
    )

    # Define campaign query
    QUERY_EDM = f"""
        SELECT
            c.HYBRIS_ID, c.Division, c.Market_Area, c.date, c.Campaign,
            c.Delivery_Success, c.Opened_Displayed, c.Clicked,
            b.open_rate AS Benchmark_OR, b.ctr AS Benchmark_CTR,
            sl.*
        FROM `xxx.gcdm.campaigns` c
            JOIN `xxx.content.subject_line` sl ON c.Email_Title = sl.subject_line
            LEFT JOIN `xxx.gcdm.benchmark` b ON c.Market_Area = b.Market_Area AND SUBSTR(c.Campaign, 26, 5) = b.Segment AND c.Channel = b.Channel
        WHERE c.HYBRIS_ID IN UNNEST(@campaign_list) AND c.Channel = 'EMAIL'

    """

    # Define click report query for each campaign
    QUERY_CLICK_REPORT = """
        SELECT
            HYBRIS_ID, Pod_adj, max(Height_pct), sum(c.Click_Rate), sum(CTR), any_value(Label_Name), any_value(Url), any_value(Pod_Position), any_value(c.Height_pct_bin), max(bm.click_rate)
        FROM
            `xxx.gcdm.click_report` c
            LEFT JOIN `xxx.content.bm_click_rate` bm ON c.Pod_Position = bm.position AND c.Height_pct_bin = bm.height_pct_bin
        WHERE
            HYBRIS_ID IN UNNEST(@campaign_list)
        GROUP BY 1,2
        ORDER BY 1,2

    """

    query_campaign = QUERY_EDM
    query_click_report = QUERY_CLICK_REPORT

    # Execute and save results of campaign query as dataframe
    df = bq_client.query(query_campaign, job_config=job_config).to_dataframe()
    df.columns = ['campaign_id', 'product', 'country', 'date', 'campaign_name', 'delivered', 'opened', 'clicked', 'bm_open_rate', 'bm_ctr', 'subject_line'] + sl.list_sl_all # Rename columns + append list of other sl features as columns
    df['country'] = df['country'].str.lower() 
    df['product'] = np.where(df['product'].isin(['VD', 'DA', 'DA, VD']), 'CE', 'MX') # Recategorize product types to just CE and MX

    # Execute and save results of click report query as dataframe
    df_click = bq_client.query(query_click_report, job_config=job_config).to_dataframe()
    if df_click.empty:
        return False
    df_click.columns = ['campaign_id', 'pod', 'height', 'click_rate', 'pod_ctr', 'label_name', 'url', 'position', 'height_bin', 'bm_click_rate'] # Rename columns
    df_click['pod_count'] = df_click.groupby('campaign_id')['pod'].transform('count') # Add column for no. of pods per campaign
    gb = df_click.groupby(['campaign_id', 'pod_count'])[['click_rate', 'pod_ctr', 'height', 'label_name']].agg(lambda x: list(x)).reset_index() # Aggregating values for each campaign

    gb['label_name'] = gb['label_name'].apply(im.truncate_labels)  #truncate the label_name 

    df = df.merge(gb, on='campaign_id', how='inner') # Merge with campaign dataframe
    if df.empty:
        return False

    data_dict = df.set_index('campaign_id').to_dict('index') #format: {'0000111111':{'country':'sg', 'curiosity':0.4, 'pod_count':3, 'click_rate':[0.3, 0.2, 0.4], ...}}

    first_campaign = next(iter(data_dict)) # Retrieve the first key from data_dict
    first_campaign_data = data_dict[first_campaign]

    not_found = [c for c in campaign_list if c not in df['campaign_id'].to_list()]

    return df, df_click, first_campaign, first_campaign_data, not_found


def get_reference_data(country, product, objective):
    """
    Fetch reference data from the `bp_edm_sl` table based on the specified country, product, and objective.

    Args:
        country (str): The country to filter the reference data.
        product (str): The product to filter the reference data.
        objective (str): The objective to filter the reference data.

    Returns:
        A DataFrame containing CUTES scores and other binary/categorical variabls.
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("country", "STRING", country),
            bigquery.ScalarQueryParameter("product", "STRING", product),
            bigquery.ScalarQueryParameter("objective", "STRING", objective)
        ]
    )

    QUERY = """
        SELECT b.* EXCEPT (country, product, objective)
        FROM `xxx.content.bp_edm_sl` b
        WHERE country = @country AND product = @product AND objective = @objective AND top_flag IN ('1', 'magnitude', 'direction')

    """

    df = bq_client.query(QUERY, job_config=job_config).to_dataframe()    

    return df


def display(df, first_campaign_img, first_campaign_data, df_ref, df_click):
    """
    Display a comprehensive campaign report with content analysis, subject line (CUTES) analysis, recommendations, and click rate analysis.

    Args:
        df: DataFrame containing campaign data.
        first_campaign_img (str): URL of the image for the first campaign.
        first_campaign_data (dict): Dictionary containing data for the first campaign, including its subject line and date.
        df_ref (pandas.DataFrame): DataFrame containing reference data for benchmarking best practices.
        df_click (pandas.DataFrame): DataFrame containing click data for pods in the campaign.

    Returns:
        None
    """
    display_width = 375 + 1000 #375 for visual + click rate bar, 1000 for analysis
    st.markdown( # CSS styling to adjust content sidth
        f"""
        <style>
        [data-testid="stVerticalBlock"]{{
            width: {display_width}px;
        }}
        [data-testid="stForm"]{{
            width: 300px;
        }}
        """,
        unsafe_allow_html=True,
    )

    open_rate = df['opened'].sum() / df['delivered'].sum()
    ctr = df['clicked'].sum() / df['opened'].sum()

    # weighted average bm - to cleanly process campaign results against benchmarks if input > 1 campaign_id
    df['bm_open_rate'] = df['bm_open_rate'] * df['delivered'] 
    df['bm_ctr'] = df['bm_ctr'] * df['opened']
    bm_open_rate = df['bm_open_rate'].sum() / df['delivered'].sum()
    bm_ctr = df['bm_ctr'].sum() / df['opened'].sum()

    # compile average cutes scores for all campaigns input
    cutes_score = df.loc[:, 'curiosity':'specificity'].mean().to_list()

    # Start of report
    st.markdown("### Campaign Content Analysis")
    st.divider()

    # Start of CUTES visualisation
    st.markdown("#### Subject Line Analysis")
    st.markdown(f"**Campaign Date:** {first_campaign_data['date']}")
    st.markdown(f"**Subject line:** {first_campaign_data['subject_line']}")
    if open_rate >= bm_open_rate:
        st.markdown(f"**Open Rate:** :green[**{open_rate:.1%}**]")
    else:
        st.markdown(f"**Open Rate:** :red[**{open_rate:.1%}**]")
    st.markdown(f"**Benchmark:** :blue[**{bm_open_rate:.1%}**]")

    # Defining left and right columns of display
    col_ratios_sl = []
    for ratio in [0.4, 0.6]: #cutes, recs
        col_ratios_sl.append(ratio)
    cols_sl = st.columns(col_ratios_sl, gap='medium')

    # cols_sl[0].markdown("**C.U.T.E.S score (tagged by Gemini)**")
    cutes_def = cols_sl[0].popover("C.U.T.E.S score (tagged by Gemini)")
    cutes_def.markdown(f'''
        *:blue[Curiosity]: Informative subject lines directly state a fact, while curiosity-inducing ones leave things unsaid, sparking interest and prompting further investigation.  
        *:blue[Urgency]: Continuous subject lines convey no sense of urgency, while those with a limited timeframe encourage immediate attention and action.  
        *:blue[Tone]: Formal subject lines are professional and distant, while casual ones are informal and friendly, making the communication feel more approachable.  
        *:blue[Emotion]: Subject lines with negative emotion focus on avoiding problems, while those with positive emotion emphasize benefits and aspirations, creating a more uplifting tone.  
        *:blue[Specificity]: Generic subject lines apply broadly to all, while personalized ones are tailored to specific interests, behaviors, or traits of the audience.  
    ''')

    fig, config = ch.make_cutes_chart(y1_data=cutes_score, y2_marker={'opacity':0}, chart_height=200)
    cols_sl[0].plotly_chart(fig, use_container_width=True, config=config)

    # Start of recommendation part
    cols_sl[1].markdown(":green[**Recommendation (based on Best Practices)**]")

    df_ref = df_ref.set_index('top_flag').transpose()
    comb = pd.concat([df[sl.list_sl_all].mean(), df_ref], axis=1) # Combined dataframe of campaign and dataframe of reference_data
    comb = comb.rename(columns={0:'campaign', '1':'top'}) # Rename columns
    comb['rec'] = (comb['campaign'] - comb['top']) * comb['magnitude'] # Calculate recommendation score, multiplying by magnitude of difference weighs the recommendation's 'importance'

    cutes = comb.loc['curiosity':'specificity', :]
    length = comb.loc['length_long':'length_short', :]
    binary = comb.loc['emoji':'ai', :]

    mapper = pd.DataFrame(sl.rec_mapper)
    if open_rate >= bm_open_rate:
    # If outperform benchmark, only 1 recommendation from CUTES and another from binary variables
        cutes = cutes[(cutes['rec'] < 0)].nsmallest(1, 'rec')
        length = length[(length['rec'] < 0) & (length['direction'] == 1)].nsmallest(0, 'rec')
        binary = binary[(binary['rec'] < 0) & (binary['direction'] == 1)].nsmallest(1, 'rec')
    else:
    # If underperform benchmark, 3 recommendations from CUTES, 1 from length, and 3 from binary variables
        cutes = cutes[(cutes['rec'] < 0)].nsmallest(3, 'rec')
        length = length[(length['rec'] < 0) & (length['direction'] == 1)].nsmallest(1, 'rec')
        binary = binary[(binary['rec'] < 0) & (binary['direction'] == 1)].nsmallest(3, 'rec')

    recs = pd.concat([cutes, length, binary])
    recs = recs.reset_index(names=['feature'])
    recs = recs.merge(mapper, on=['feature', 'direction'], how='left')

    for i, m in enumerate(recs['message'].to_list()):
        cols_sl[1].markdown(f"{i+1}. {m}")

    st.divider()

    # Start of content visual analysis
    st.markdown("#### Content Analysis")

    # Defining display column setup to control layout
    col_ratios_edm = []
    for ratio in [0.22, 0.08, 0.70]: #visual, click bar, analysis
        col_ratios_edm.append(ratio)
    cols = st.columns(col_ratios_edm, gap='medium')

    # Defining display within each column
    cols[0].image(first_campaign_img, width=300)
    cols[1].image(im.draw_click_rate_bar(first_campaign_img, first_campaign_data, click_data_type='Pod click contribution'), width=75)
    cols[2].markdown("**Click rate analysis**")

    # Filter out footer data from click report
    df_click = df_click[df_click['position'] != 'Footers']
    gb_position = df_click.groupby(['position'], as_index=False)[['click_rate', 'bm_click_rate']].mean() # Calculating average and benchmark click rates for each pod position
    gb_position = gb_position.sort_values('position', ascending=False)
    gb_relative_height = df_click.groupby(['height_bin'], as_index=False)[['click_rate', 'bm_click_rate']].mean() # Calculate average statistics for each relative height
    gb_relative_height = gb_relative_height.sort_values('height_bin', ascending=True) 

    # Generate click rate chart
    fig = ch.make_click_rate_chart(groups={'position':gb_position, 'height_bin':gb_relative_height})
    cols[2].plotly_chart(fig, use_container_width=False)

    # Display top performing pod based on click contribution
    cols[2].markdown("**Top performing pod**")

    # Calculate difference between actual and bm for each to find top performing based on highest difference
    df_click['pod_perf'] = df_click['click_rate'] - df_click['bm_click_rate']
    top_pod_click_rate = format(df_click.loc[df_click['pod_perf'].idxmax(), 'click_rate'], ".1%")
    top_pod_bm = format(df_click.loc[df_click['pod_perf'].idxmax(), 'bm_click_rate'], ".1%")
    top_pod_url = df_click.loc[df_click['pod_perf'].idxmax(), 'url']

    # Try to display image of top-performing pod, else display error message
    try:
        cols[2].image(top_pod_url, width=300)
        cols[2].markdown(f"Pod click rate is {top_pod_click_rate}. Similar pods click rate is {top_pod_bm}.")
    except:
        cols[2].markdown("*Errors fetching image!*")

    return


def main():
    if submit_campaign_id:
        data_tuple = get_campaign_data(campaign_id=campaign_id) #return a tuple of (df, first campaign ID, first campaign data, not_found)
        if data_tuple:
            df, df_click, first_cp_id, first_cp_data, not_found = data_tuple[0], data_tuple[1], data_tuple[2], data_tuple[3], data_tuple[4]

            if df.shape[0] > 1: # If more than 1 campaign is found, deal with each scenario
                st.write("You searched for multiple campaigns. Only 1st campaign's creatives will be displayed. Analysis will still be performed for all campaigns.")
                if not_found:
                    st.write(f"These campaign IDs cannot be found {', '.join(not_found)}")

            country = first_cp_data['country'].upper()
            product = first_cp_data['product']
            # product = cp.get_product_from_model(first_cp_data['model'])
            df_ref = get_reference_data(country=country, product=product, objective=campaign_obj)

            # Fetch campaign images using first campaign's ID and data from storage bucket
            img_dict = im.get_img_from_dict({first_cp_id:first_cp_data}, storage_client=storage_client, bucket_name=edm_bucket) #get img using 1st camp data
            if img_dict:
                display(df=df, df_click=df_click, first_campaign_img=img_dict[first_cp_id], first_campaign_data=first_cp_data, df_ref=df_ref)
            else:
                st.write('The creatives for searched campaigns have not been updated yet!')
        else:
            st.write('The search did not return any campaign!')


main()