import streamlit as st

import pandas as pd
import numpy as np

from datetime import datetime
import datetime

from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

import core.img_utils as im
import core.cp_utils as cp


st.set_page_config(layout='wide', page_title='CAP - Content Analysis Platform')

credentials = service_account.Credentials.from_service_account_file('xxx.json')
storage_client = storage.Client(project='xxx', credentials=credentials)
bq_client = bigquery.Client(project='xxx', credentials=credentials)
edm_bucket = 'creative-edm'
pn_bucket = 'creative-push'


with st.sidebar:
    st.header("SEAO Content Analytics")

    today = datetime.datetime.now()
    # last_year = today.year - 1

    start_date = datetime.date((today.year), 1, 1)
    end_date = datetime.date(today.year, 12, 31) 

    form_market_date = st.form(key='market_date')
    market = form_market_date.selectbox('Market',('SG',  'ID', 'MY', 'NZ', 'PH', 'VN'))
    date = form_market_date.date_input(
        "Select Campaign Date",
        (start_date, datetime.date(today.year, 1, 7)),  # Default value: Today's date
        start_date,
        end_date,
        format="DD.MM.YYYY",
    )
    channel_2 = form_market_date.selectbox('Channel', ('EMAIL', 'PUSH'))
    submit_market_date = form_market_date.form_submit_button(label='Search')

    st.write('OR')

    form_campaign_id = st.form(key='campaign_id')
    campaign_id = form_campaign_id.text_input("Enter list of campaign IDs", placeholder='0000111111, 0000222222')
    channel_1 = form_campaign_id.selectbox('Channel', ('EMAIL', 'PUSH'))
    submit_campaign_id = form_campaign_id.form_submit_button(label='Search')

    st.write('Optional filters')

    click_rate_display = st.selectbox('Click Contribution Option:', ('Normal', 'Exclude Footers', 'Exclude Footers - Keep Unsubscribe'))
    click_data_type = st.selectbox('Pod Click Metric:', ('Pod click contribution', 'Pod CTR'))
    sorting = st.selectbox('Sorting options:', ('Segment','Campaign ID', 'Market', 'Campaign Date', 'Sent', 'OR', 'CTR'), index = 3) #set order by date as defaut



def get_campaign_data(channel, click_rate_display, sorting, campaign_id=None, market=None, date=None):
    if campaign_id is not None:
        campaign_list = cp.parse_campaign_id(campaign_id)
        job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("campaign_list", "STRING", campaign_list),
            bigquery.ScalarQueryParameter("market", "STRING", market)
            # bigquery.ScalarQueryParameter("start_date", "DATE", start_date_selected),
            # bigquery.ScalarQueryParameter("end_date", "DATE", end_date_selected),
        ]
    )
        
    else:
        campaign_list = []
        start_date_selected = date[0]
        end_date_selected = date[1]
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("campaign_list", "STRING", campaign_list),
                bigquery.ScalarQueryParameter("market", "STRING", market),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date_selected),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date_selected),
            ]
        )

    # # Mapping the selected filter option to SQL    
    # where_clause = ""
    # if campaign_id is not None:
    #     where_clause = f"WHERE c.HYBRIS_ID IN UNNEST(@campaign_list) "
    # else:
    #     # where_clause = f"WHERE c.Market_Area = @market AND c.date = @date"
    #     where_clause = f"WHERE c.country = @market AND c.date BETWEEN @start_date AND @end_date"


    where_clause = ""
    if campaign_id is not None:
        where_clause = f"WHERE c.HYBRIS_ID IN UNNEST(@campaign_list) "
    else:
        where_clause = f"WHERE c.Market_Area = @market AND c.date = @date"

    # Mapping the selected sorting option to SQL
    order_by_clause = "ORDER BY "
    if sorting == 'Campaign ID':
        order_by_clause += "c.HYBRIS_ID"
    elif sorting == 'Market':
        order_by_clause += "Market_Area"
    elif sorting == 'Sent':
        order_by_clause += "Delivery_Success DESC"
    elif sorting == 'OR':
        order_by_clause += "Opened_Displayed/Delivery_Success DESC"
    elif sorting == 'CTR':
        order_by_clause += "Clicked/Opened_Displayed DESC"
    elif sorting == 'Segment':
        order_by_clause += "Segment"
    elif sorting == 'Campaign Date':
        order_by_clause += "date"


    # Mapping the selected filter option to SQL
    select_clause = ""
    if channel == 'EMAIL':
        select_clause = "c.Email_Title"
    else:
        select_clause = "p.ticker, p.text"


    QUERY = f"""
        SELECT
            c.Market_Area, c.HYBRIS_ID, {select_clause}, c.date, c.Campaign, c.Segment,
            c.Delivery_Success, c.Opened_Displayed, c.Clicked
        FROM `xxx.gcdm.campaigns` c
        LEFT JOIN `xxx.gcdm.campaign_asset_push` p
        ON c.HYBRIS_ID = p.HYBRIS_ID
        {where_clause} AND c.Channel = '{channel}'
        {order_by_clause}
    """

    # QUERY_EDM = f"""
    #     SELECT
    #         country, HYBRIS_ID, Email_Title, date, Campaign_Name, Segment_Benchmark,
    #         Targeted_Sent, Opened_Displayed, Clicked
    #     FROM `xxx.DASHBOARD_FLAGSHIP.PARADIGM_CAMPAIGN_PERFORMANCE_EDM` c
    #     {where_clause} AND is_latest_rank = 1
    #     {order_by_clause}
    # """

    # QUERY_PN = f"""
    #     SELECT
    #         country, c.HYBRIS_ID, ticker, text, date, Campaign_Name, Segment,
    #         SMP_Sent, SMP_Displayed, SMP_Clicked
    #     FROM `xxx.DASHBOARD_FLAGSHIP.PARADIGM_CAMPAIGN_PERFORMANCE_PUSH` c
    #     JOIN `xxx.gcdm.campaign_asset_push` p 
    #     ON c.HYBRIS_ID = p.HYBRIS_ID
    #     {where_clause} AND is_latest_rank = 1
    #     {order_by_clause}

    # """

    # QUERY_CLICK_REPORT = f"""
    #     SELECT cr.HYBRIS_ID, Pod_adj, max(Height_pct), sum(Click_Rate), sum(CTR), sum(CTR_With_Unsubscribe), coalesce(sum(CR_Excl_Footer), 0), sum(CR_With_Unsubscribe), any_value(Label_Name)
    #     FROM `xxx.gcdm.click_report` cr
    #     JOIN `xxx.DASHBOARD_FLAGSHIP.PARADIGM_CAMPAIGN_PERFORMANCE_EDM` c
    #     ON cr.HYBRIS_ID = c.HYBRIS_ID
    #     {where_clause}
    #     GROUP BY 1,2
    #     ORDER BY 1,2

    # """


    QUERY_CLICK_REPORT = f"""
        SELECT HYBRIS_ID, Pod_adj, max(Height_pct), sum(Click_Rate), sum(CTR), sum(CTR_With_Unsubscribe), coalesce(sum(CR_Excl_Footer), 0), sum(CR_With_Unsubscribe), any_value(Label_Name)
        FROM `xxx.gcdm.click_report` c
        {where_clause}
        GROUP BY 1,2
        ORDER BY 1,2

    """


    df = bq_client.query(QUERY, job_config=job_config).to_dataframe()

    if channel == 'EMAIL':
        # df = bq_client.query(QUERY_EDM, job_config=job_config).to_dataframe()
        df.columns = ['country', 'campaign_id', 'subject_line', 'date', 'campaign_name','segment_name', 'delivered', 'engaged', 'clicked']
    else:
        # df = bq_client.query(QUERY_PN, job_config=job_config).to_dataframe()
    
        df.columns = ['country', 'campaign_id', 'ticker', 'text', 'date', 'campaign_name','segment_name', 'delivered','engaged', 'clicked']
    df['open_rate'] = (df['engaged'] / df['delivered'] * 100).round(1).astype(str) + '%'
    df['CTR'] = (df['clicked'] / df['engaged'] * 100).round(1).astype(str) + '%'
    df['country'] = df['country'].str.lower()


    if channel == 'EMAIL':
        df_click = bq_client.query(QUERY_CLICK_REPORT, job_config=job_config).to_dataframe()
        if df_click.empty:
            st.write('The search did not return any campaign. Please try a different search!')
            return False

        df_click.columns = ['campaign_id', 'pod', 'height', 'click_rate', 'pod_ctr', 'pod_ctr_with_unsub', 'click_rate_excl_footer', 'click_rate_with_unsub', 'label_name']
        df_click['pod_count'] = df_click.groupby('campaign_id')['pod'].transform('count')
        if click_rate_display == 'Normal':
            click_rate = 'click_rate'
            pod_ctr = 'pod_ctr'
        elif click_rate_display == 'Exclude Footers':
            click_rate = 'click_rate_excl_footer'
            pod_ctr = 'pod_ctr'
        else:
            click_rate = 'click_rate_with_unsub'
            pod_ctr = 'pod_ctr_with_unsub'
            df_click['label_name'] = np.where(df_click['label_name'] == 'footer', 'unsub', df_click['label_name'])

        gb = df_click.groupby(['campaign_id', 'pod_count'])[[click_rate, pod_ctr, 'height', 'label_name']].agg(lambda x: list(x)).reset_index()
        gb.columns = ['campaign_id', 'pod_count', 'click_rate', 'pod_ctr', 'height', 'label_name']

        gb['label_name'] = gb['label_name'].apply(im.truncate_labels)  #truncate label_name

        data = df.merge(gb, on='campaign_id', how='inner')
    else:
        data = df
    data_dict = data.set_index('campaign_id').to_dict('index') #format: {'0000111111':{'country':'sg', 'curiosity':0.4, 'pod_count':3, 'click_rate':[0.3, 0.2, 0.4], ...}}

    return data_dict


def display(channel, img_dict, data_dict, click_data_type):
    if channel == 'EMAIL':
        unit_length = 450
    else:
        unit_length = 350

    display_width = unit_length * len(img_dict)
    st.markdown(
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

    col_ratios = []
    for i in range(len(img_dict)):
        if channel == 'EMAIL': #80% for image, 20% for click rate bar
            col_ratios.append(0.75/len(img_dict))
            col_ratios.append(0.25/len(img_dict))
        else:
            col_ratios.append(0.99/len(img_dict))
            col_ratios.append(0.01)
    cols = st.columns(col_ratios, gap='medium')

    for i, (k, v) in enumerate(img_dict.items()):
        cols[i*2].write(f"{k} | {data_dict[k]['country'].upper()} | {data_dict[k]['date']}")
        cols[i*2].text(f"{data_dict[k]['campaign_name']}")
        cols[i*2].text(f"{data_dict[k]['segment_name']}")

        if channel == 'EMAIL':
            cols[i*2].text(f"{data_dict[k]['subject_line']}")
            cols[i*2].text(f"Sent: {cp.human_format(data_dict[k]['delivered'])} | OR: {data_dict[k]['open_rate']} | CTR: {data_dict[k]['CTR']}")
        else:
            cols[i*2].text(f"Ticker: {data_dict[k]['ticker']}")
            cols[i*2].text(f"Text: {data_dict[k]['text']}")
            cols[i*2].text(f"Displayed: {cp.human_format(data_dict[k]['delivered'])} | CTR: {data_dict[k]['CTR']}")
            cols[i*2].text('CTR is the percentage of displayed users who clicked Push notifs')    
        
        cols[i*2].image(img_dict[k], width=300)

        if channel == 'EMAIL':
            for x in range(5):
                cols[i*2+1].text("|")
            cols[i*2+1].image(im.draw_click_rate_bar(img_dict[k], data_dict[k], click_data_type=click_data_type), width=75) 

    return


def main():
    if submit_campaign_id:
        data_dict = get_campaign_data(channel=channel_1, click_rate_display=click_rate_display, sorting=sorting, campaign_id=campaign_id)
        
        if channel_1 == 'EMAIL':
            bucket = edm_bucket

        else:
            bucket = pn_bucket

        if data_dict:
            img_dict = im.get_img_from_dict(data_dict=data_dict, storage_client=storage_client, bucket_name=bucket)
            display(channel=channel_1, img_dict=img_dict, data_dict=data_dict, click_data_type=click_data_type)
    if submit_market_date:
        data_dict = get_campaign_data(channel=channel_2, click_rate_display=click_rate_display, sorting=sorting, market=market, date=date)

        if channel_2 == 'EMAIL':
            bucket = edm_bucket
        else:
            bucket = pn_bucket

        if data_dict:
            img_dict = im.get_img_from_dict(data_dict=data_dict, storage_client=storage_client, bucket_name=bucket)
            display(channel=channel_2, img_dict=img_dict, data_dict=data_dict, click_data_type=click_data_type)


main()