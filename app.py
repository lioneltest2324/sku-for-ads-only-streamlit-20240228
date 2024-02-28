import streamlit as st
import pandas as pd
import numpy as np
from universal_component_for_campaign import load_and_process_data,process_usfeed_and_hmfeed_sku_on_ads_data,process_hk_cost_and_value_on_ads_data,\
    process_old_new_sku_2022_and_2023_on_ads_data,merged_spu_to_sku_on_ads_data,merged_imagelink_to_sku_on_ads_data,create_date_filtered_df,\
    output_groupby_df,out_date_range_data,add_groupby_sum_columns_to_list_df,create_dynamic_column_setting,add_custom_proportion_to_df,add_custom_proportion_to_df_x100,\
    create_sensor_gmv_filter_input,create_bulk_sku_input,create_sensor_campaign_filter_input_df,condition_evaluate,merged_saleprice_to_sku_on_ads_data,\
    create_compare_summary_df,format_first_two_rows,format_comparison,colorize_comparison
st.set_page_config(layout="wide")
ads_url = 'https://docs.google.com/spreadsheets/d/1OAA3t9dnj5rexmBcaQXttNVLvNz6OYteL-6NGZICX7M/edit#gid=0'
spu_index_url = "https://docs.google.com/spreadsheets/d/1bQTrtNC-o9etJ3xUwMeyD8m383xRRq9U7a3Y-gxjP-U/edit#gid=455883801"

@st.cache_data(ttl=2400)
def add_custom_cul_proportion_to_df(df,A_COLUMNS,B_COLUMNS,CUSTOM_COLUMNS_NAME):
    df[CUSTOM_COLUMNS_NAME] = (df[A_COLUMNS] / df[B_COLUMNS])-1
    df[CUSTOM_COLUMNS_NAME] = df[CUSTOM_COLUMNS_NAME].fillna(0)  # 将NaN替换为0
    df[CUSTOM_COLUMNS_NAME] = df[CUSTOM_COLUMNS_NAME].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    return df

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
with st.sidebar:
    selected_range = out_date_range_data(ads_daily_df, 'Date', "自选日期范围")
    compare_selected_range = out_date_range_data(ads_daily_df, 'Date', "对比数据日期范围")
    start_date = pd.to_datetime(selected_range[0])
    end_date = pd.to_datetime(selected_range[1])
    compare_end_date = (start_date - pd.Timedelta(days=1)).strftime('%Y/%m/%d')
    compare_start_date = (start_date - pd.Timedelta(days=1) - pd.Timedelta(end_date-start_date)).strftime('%Y/%m/%d')
    st.subheader(f"提示：当前自选日期的上个环比周期为{compare_start_date}—{compare_end_date}")
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
# 三级类目SKU数据对比
# 先处理对比数据
st.subheader('三级类目')
unique_category_3 = ads_daily_df['Product Type 3'].unique()
category_3_options = st.multiselect(
    '选择三级类目',
    unique_category_3
)
st.text("增长值为0的特殊情况:对比日期范围内的数据为0或无数据")
compare_ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',compare_selected_range)
compare_ads_daily_filtered_date_range_df = output_groupby_df(compare_ads_daily_filtered_date_range_df,
['SKU', 'Date', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
compare_ads_summary_filtered_date_range_df = output_groupby_df(compare_ads_daily_filtered_date_range_df,
['SKU', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
ads_summary_filter_category_3_select_df = ads_summary_filtered_date_range_output_df[ads_summary_filtered_date_range_output_df['Product Type 3'].isin(category_3_options)]
compare_ads_summary_filter_category_3_select_df = compare_ads_summary_filtered_date_range_df[compare_ads_summary_filtered_date_range_df['Product Type 3'].isin(category_3_options)]
category_3_summary_df = pd.merge(ads_summary_filter_category_3_select_df,compare_ads_summary_filter_category_3_select_df
[['SKU','impression', 'cost', 'click', 'conversions', 'ads value']],on=['SKU'], how='left')
cost_sum = category_3_summary_df['cost_x'].sum()
ads_value_sum = category_3_summary_df['ads value_x'].sum()
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'ads value_x', 'cost_x', '自选日期内ads-ROI')
category_3_summary_df['自选日期内该三级类目平均ads-ROI'] = ads_value_sum/cost_sum

category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'impression_x','impression_y','impression增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'cost_x','cost_y','cost增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'click_x','click_y','click增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'conversions_x','conversions_y','conversions增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'ads value_x','ads value_y','ads value增长值')
remove_category_3_summary_df = category_3_summary_df.drop(
columns=[ 'Product Type 1', 'Product Type 2', 'Product Type 3','impression_x','impression_y','click_x','click_y','conversions_x','conversions_y','ads value_x','ads value_y'])
remove_category_3_summary_df['impression增长值'] = remove_category_3_summary_df['impression增长值'].map(lambda x: f'{x:.2%}')
remove_category_3_summary_df['cost增长值'] = remove_category_3_summary_df['cost增长值'].map(lambda x: f'{x:.2%}')
remove_category_3_summary_df['click增长值'] = remove_category_3_summary_df['click增长值'].map(lambda x: f'{x:.2%}')
remove_category_3_summary_df['conversions增长值'] = remove_category_3_summary_df['conversions增长值'].map(lambda x: f'{x:.2%}')
remove_category_3_summary_df['ads value增长值'] = remove_category_3_summary_df['ads value增长值'].map(lambda x: f'{x:.2%}')
remove_category_3_summary_df= remove_category_3_summary_df.rename(columns={'cost_x':'自选日期内花费','cost_y':'对比日期内花费'})
column_config={"imagelink": st.column_config.ImageColumn(width="small")}
remove_category_3_summary_df = remove_category_3_summary_df.sort_values(by='自选日期内花费', ascending=False)
st.dataframe(remove_category_3_summary_df.set_index(['SKU','imagelink']),height=600, width=2400,column_config=column_config)
