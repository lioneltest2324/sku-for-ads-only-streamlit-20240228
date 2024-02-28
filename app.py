import streamlit as st
import pandas as pd
from universal_component_for_campaign import load_and_process_data,process_usfeed_and_hmfeed_sku_on_ads_data,process_hk_cost_and_value_on_ads_data,\
    process_old_new_sku_2022_and_2023_on_ads_data,merged_spu_to_sku_on_ads_data,merged_imagelink_to_sku_on_ads_data,create_date_filtered_df,\
    output_groupby_df,out_date_range_data,add_groupby_sum_columns_to_list_df,create_dynamic_column_setting,add_custom_proportion_to_df,add_custom_proportion_to_df_x100,\
    create_sensor_gmv_filter_input,create_bulk_sku_input,create_sensor_campaign_filter_input_df,condition_evaluate,merged_saleprice_to_sku_on_ads_data,\
    create_compare_summary_df,format_first_two_rows,format_comparison,colorize_comparison
st.set_page_config(layout="wide")
ads_url = 'https://docs.google.com/spreadsheets/d/1OAA3t9dnj5rexmBcaQXttNVLvNz6OYteL-6NGZICX7M/edit#gid=0'
spu_index_url = "https://docs.google.com/spreadsheets/d/1bQTrtNC-o9etJ3xUwMeyD8m383xRRq9U7a3Y-gxjP-U/edit#gid=455883801"

ads_daily_df = load_and_process_data(ads_url,0)
spu_index = load_and_process_data(spu_index_url,455883801)
old_new = load_and_process_data(spu_index_url,666585210)
ads_daily_df= process_usfeed_and_hmfeed_sku_on_ads_data(ads_daily_df,'MC ID',569301767,9174985,'SKU')
ads_daily_df= process_hk_cost_and_value_on_ads_data(ads_daily_df,'Currency','cost','ads value','HKD')
ads_daily_df = process_old_new_sku_2022_and_2023_on_ads_data(ads_daily_df,'customlabel1')
ads_daily_df['SKU'] = ads_daily_df['SKU'].str.strip().str.replace('\n', '').replace('\t', '').str.upper()
old_new['SKU ID'] = old_new['SKU ID'].str.strip().str.replace('\n', '').replace('\t', '').str.upper()
old_new  = old_new.rename(columns={'SKU ID':'SKU'})
ads_daily_df = merged_imagelink_to_sku_on_ads_data(ads_daily_df,old_new,'SKU', 'imagelink')
ads_daily_df = merged_saleprice_to_sku_on_ads_data(ads_daily_df,old_new,'SKU', 'Sale Price')
# 日期选择框
selected_range = out_date_range_data(ads_daily_df, 'Date', "自选日期范围")
compare_selected_range = out_date_range_data(ads_daily_df, 'Date', "对比数据日期范围")
# 选择日期范围内的数据
ads_daily_df['Date'] = pd.to_datetime(ads_daily_df['Date'])
# 处理普通选择日期范围内的数据
ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',selected_range)
# 日维度数据源下载
ads_daily_filtered_date_range_output_df = output_groupby_df(ads_daily_filtered_date_range_df,
['SKU', 'Date', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
ads_daily_filtered_date_range_output_df['Date'] = ads_daily_filtered_date_range_output_df['Date'].dt.strftime('%Y-%m-%d')
st.subheader('数据源下载')
download_daily_expander = st.expander("筛选时间内全SKU日维度数据下载")
daily_csv_data = ads_daily_filtered_date_range_output_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
download_daily_expander.dataframe(ads_daily_filtered_date_range_output_df,height=300, width=2400)
download_daily_expander.download_button(
        label="日维度数据下载",
        data=daily_csv_data,
        file_name='日维度SKU数据.csv',
        mime='text/csv',
    )
# 汇总数据源下载
ads_summary_filtered_date_range_output_df = output_groupby_df(ads_daily_filtered_date_range_df,
['SKU', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
download_summary_expander = st.expander("筛选时间内全SKU汇总数据下载")
summary_csv_data = ads_summary_filtered_date_range_output_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
download_summary_expander.dataframe(ads_summary_filtered_date_range_output_df,height=300, width=2400)
download_summary_expander.download_button(
        label="汇总数据下载",
        data=daily_csv_data,
        file_name='汇总SKU数据.csv',
        mime='text/csv',
    )
