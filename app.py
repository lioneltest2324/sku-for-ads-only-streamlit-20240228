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

@st.cache_data(ttl=2400)
def add_custom_cul_single_proportion_to_df(df,A_COLUMNS,B_COLUMNS,CUSTOM_COLUMNS_NAME):
    df[CUSTOM_COLUMNS_NAME] = (df[A_COLUMNS] / df[B_COLUMNS])
    df[CUSTOM_COLUMNS_NAME] = df[CUSTOM_COLUMNS_NAME].fillna(0)  # 将NaN替换为0
    df[CUSTOM_COLUMNS_NAME] = df[CUSTOM_COLUMNS_NAME].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    return df

ads_daily_df = load_and_process_data(ads_url,0)

sensor_data = load_and_process_data(ads_url,1408314368)
spu_index = load_and_process_data(spu_index_url,455883801)
old_new = load_and_process_data(spu_index_url,666585210)

ads_daily_df= process_usfeed_and_hmfeed_sku_on_ads_data(ads_daily_df,'MC ID',569301767,9174985,'SKU')
old_new['SKU ID'] = old_new['SKU ID'].str.strip().str.replace('\n', '').replace('\t', '').str.upper()
ads_daily_df['SKU'] = ads_daily_df['SKU'].str.strip().str.replace('\n', '').replace('\t', '').str.upper()
sensor_data  = sensor_data.rename(columns={'日期':'Date','三级类目':'Product Type 3','GMV':'神策三级类目GMV'})
sensor_data['Product Type 3'] = sensor_data['Product Type 3'].str.lower()

old_new  = old_new.rename(columns={'SKU ID':'SKU'})
ads_daily_df = ads_daily_df.drop(columns=['customlabel1'])
ads_daily_df = pd.merge(ads_daily_df,old_new[['SKU','customlabel1']],on=['SKU'], how='left')
ads_daily_df= process_hk_cost_and_value_on_ads_data(ads_daily_df,'Currency','cost','ads value','HKD')
ads_daily_df = process_old_new_sku_2022_and_2023_on_ads_data(ads_daily_df,'customlabel1')
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
sensor_data['Date'] = pd.to_datetime(sensor_data['Date'])
# 处理普通选择日期范围内的数据
ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',selected_range)
# 根据日期处理神策三级类目GMV日维度表
sensor_daily_filtered_date_range_df = create_date_filtered_df(sensor_data,'Date',selected_range)
# 根据神策三级类目GMV日维度表做汇总表
sensor_summary_filtered_date_range_df = output_groupby_df(sensor_daily_filtered_date_range_df,
['Product Type 3'],['神策三级类目GMV'], 'sum').reset_index()
# 根据日期处理神策三级类目GMV日维度表(对比)
compare_sensor_daily_filtered_date_range_df = create_date_filtered_df(sensor_data,'Date',compare_selected_range)
# 根据神策三级类目GMV日维度表做汇总表(对比)
compare_sensor_summary_filtered_date_range_df = output_groupby_df(compare_sensor_daily_filtered_date_range_df,
['Product Type 3'],['神策三级类目GMV'], 'sum').reset_index()



# 日维度数据源下载
ads_daily_filtered_date_range_output_df = output_groupby_df(ads_daily_filtered_date_range_df,
['SKU', 'Date', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
ads_daily_filtered_date_range_output_df = pd.merge(ads_daily_filtered_date_range_output_df,spu_index[['SKU','SPU']],on=['SKU'], how='left')
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
ads_summary_filtered_date_range_output_df = pd.merge(ads_summary_filtered_date_range_output_df,spu_index[['SKU','SPU']],on=['SKU'], how='left')
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
st.subheader('三级类目对比数据')
unique_category_3 = ads_daily_df['Product Type 3'].unique()
category_3_options = st.multiselect(
    '选择三级类目',
    unique_category_3
)
compare_ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',compare_selected_range)
compare_ads_daily_filtered_date_range_df = output_groupby_df(compare_ads_daily_filtered_date_range_df,
['SKU', 'Date', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
compare_ads_summary_filtered_date_range_df = output_groupby_df(compare_ads_daily_filtered_date_range_df,
['SKU', 'Product Type 1', 'Product Type 2', 'Product Type 3','old_or_new', 'imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
# 根据选项做三级类目筛选日维度表(日维度底表)
ads_daily_filter_category_3_select_df = ads_daily_filtered_date_range_df[ads_daily_filtered_date_range_df['Product Type 3'].isin(category_3_options)]
compare_ads_daily_filter_category_3_select_df = compare_ads_daily_filtered_date_range_df[compare_ads_daily_filtered_date_range_df['Product Type 3'].isin(category_3_options)]
# 三级类目日维度花费趋势表(图表专用)
ads_daily_filter_category_3_trend_df = output_groupby_df(ads_daily_filter_category_3_select_df,['Date'],['cost'], 'sum').reset_index()
ads_daily_filter_category_3_trend_df['Date'] = ads_daily_filter_category_3_trend_df['Date'].dt.strftime('%Y-%m-%d')
# 三级类目日维度花费对比趋势表(图表专用)
compare_ads_daily_filter_category_3_trend_df = output_groupby_df(compare_ads_daily_filter_category_3_select_df,['Date'],['cost'], 'sum').reset_index()
compare_ads_daily_filter_category_3_trend_df['Date'] = compare_ads_daily_filter_category_3_trend_df['Date'].dt.strftime('%Y-%m-%d')
# 根据选项做三级类目筛选SKU表(汇总表)
sensor_summary_filter_category_3_select_df = sensor_summary_filtered_date_range_df[sensor_summary_filtered_date_range_df['Product Type 3'].isin(category_3_options)]
compare_sensor_summary_filter_category_3_select_df = compare_sensor_summary_filtered_date_range_df[compare_sensor_summary_filtered_date_range_df['Product Type 3'].isin(category_3_options)]

ads_summary_filter_category_3_select_df = ads_summary_filtered_date_range_output_df[ads_summary_filtered_date_range_output_df['Product Type 3'].isin(category_3_options)]
compare_ads_summary_filter_category_3_select_df = compare_ads_summary_filtered_date_range_df[compare_ads_summary_filtered_date_range_df['Product Type 3'].isin(category_3_options)]
category_3_summary_df = pd.merge(ads_summary_filter_category_3_select_df,compare_ads_summary_filter_category_3_select_df
[['SKU','impression', 'cost', 'click', 'conversions', 'ads value']],on=['SKU'], how='left')
category_3_summary_df = pd.merge(category_3_summary_df,sensor_summary_filter_category_3_select_df
[['Product Type 3','神策三级类目GMV']],on=['Product Type 3'], how='left')
category_3_summary_df = pd.merge(category_3_summary_df,compare_sensor_summary_filter_category_3_select_df
[['Product Type 3','神策三级类目GMV']],on=['Product Type 3'], how='left')
category_3_summary_df = category_3_summary_df.rename(columns={'神策三级类目GMV_x':'自选日期该三级类目神策GMV'})
category_3_summary_df = category_3_summary_df.rename(columns={'神策三级类目GMV_y':'对比日期该三级类目神策GMV'})

cost_sum = category_3_summary_df['cost_x'].sum()
sensor_sum = category_3_summary_df['自选日期该三级类目神策GMV'].iloc[0]
compare_sensor_sum = category_3_summary_df['对比日期该三级类目神策GMV'].iloc[0]
compare_cost_sum = category_3_summary_df['cost_y'].sum()
ads_value_sum = category_3_summary_df['ads value_x'].sum()
compare_ads_value_sum = category_3_summary_df['ads value_y'].sum()
conversion_sum = category_3_summary_df['conversions_x'].sum()
compare_conversion_sum = category_3_summary_df['conversions_y'].sum()
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'ads value_x', 'cost_x', '自选日期ads-ROI')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'ads value_y', 'cost_y', '对比日期ads-ROI')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'cost_x', 'click_x', '自选日期CPC')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'cost_y', 'click_y', '对比日期CPC')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'click_x', 'impression_x', '自选日期CTR')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'click_y', 'impression_y', '对比日期CTR')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'conversions_x', 'click_x', '自选日期CVR')
category_3_summary_df = add_custom_proportion_to_df(category_3_summary_df, 'conversions_y', 'click_y', '对比日期CVR')
category_3_summary_df['自选日期该三级类目神策ROI'] = sensor_sum/cost_sum
category_3_summary_df['对比日期该三级类目神策ROI'] = compare_sensor_sum/compare_cost_sum
category_3_summary_df['自选日期内该三级类目平均ads-ROI'] = ads_value_sum/cost_sum
category_3_summary_df['cost_sum'] = cost_sum
category_3_summary_df['compare_cost_sum'] = compare_cost_sum
category_3_summary_df['ads_value_sum'] = ads_value_sum
category_3_summary_df['compare_ads_value_sum'] = compare_ads_value_sum
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'自选日期CVR','对比日期CVR','CVR增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'impression_x','impression_y','展示次数增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'自选日期CTR','对比日期CTR','CTR增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'cost_x','cost_y','cost增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'click_x','click_y','click增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'conversions_x','conversions_y','转化次数增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'ads value_x','ads value_y','ads value增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'自选日期ads-ROI','对比日期ads-ROI','ads ROI增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'自选日期CPC','对比日期CPC','CPC增长值')
category_3_summary_df = add_custom_cul_single_proportion_to_df(category_3_summary_df,'cost_x','cost_sum','自选日期花费占比')
category_3_summary_df = add_custom_cul_single_proportion_to_df(category_3_summary_df,'cost_y','compare_cost_sum','对比日期花费占比')
category_3_summary_df = add_custom_cul_single_proportion_to_df(category_3_summary_df,'ads value_x','ads_value_sum','自选日期ads value占比')
category_3_summary_df = add_custom_cul_single_proportion_to_df(category_3_summary_df,'ads value_y','compare_ads_value_sum','对比日期ads value占比')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'自选日期花费占比','对比日期花费占比','花费占比增长值')
category_3_summary_df = add_custom_cul_proportion_to_df(category_3_summary_df,'自选日期ads value占比','对比日期ads value占比','ads value占比增长值')
remove_category_3_summary_df = category_3_summary_df.drop(
columns=[ 'Product Type 1', 'Product Type 2', 'Product Type 3','impression_x','impression_y','click_x','click_y','conversions_x','conversions_y'])
remove_category_3_summary_df= remove_category_3_summary_df.rename(columns={'cost_x':'自选日期花费','cost_y':'对比日期花费','ads value_x':'自选日期ads value','ads value_y':'对比日期ads value'})

remove_category_3_summary_df = remove_category_3_summary_df.sort_values(by='自选日期花费', ascending=False)
category_3_compare_select_options = st.multiselect(
    '选择数据维度',
    remove_category_3_summary_df.columns,
    ['SKU', 'imagelink', 'old_or_new', '自选日期花费', '对比日期花费','自选日期ads value','对比日期ads value','cost增长值','ads value增长值',
     'ads ROI增长值','转化次数增长值','CPC增长值','CVR增长值','CTR增长值','自选日期花费占比','对比日期花费占比']
)
st.text(f"自选日期三级类目花费总和为：{round(cost_sum,2)},对比日期三级类目花费总和为：{round(compare_cost_sum,2)}")
st.text(f"自选日期三级类目adsvalue总和为：{round(ads_value_sum,2)},对比日期三级类目adsvalue总和为：{round(compare_ads_value_sum,2)}")
st.text(f"自选日期三级类目conversion总和为：{round(conversion_sum,2)},对比日期三级类目conversion总和为：{round(compare_conversion_sum,2)}")
st.text(f"自选日期三级类目aov为：{round(ads_value_sum/conversion_sum,2)},对比日期三级类目aov总和为：{round(compare_ads_value_sum/compare_conversion_sum,2)}")
st.text("增长值为0的特殊情况:对比日期范围内的数据为0或无数据")
st.subheader(f"{selected_range[0]}至{selected_range[1]}对比{compare_selected_range[0]}至{compare_selected_range[1]}")
format_dict = {
    'cost增长值':'{0:.2%}','CPC增长值':'{0:.2%}','impression增长值':'{0:.2%}','转化次数增长值':'{0:.2%}','花费占比增长值':'{0:.2%}','ads value占比增长值':'{0:.2%}',
    'CTR增长值':'{0:.2%}','自选日期CTR':'{0:.2%}','对比日期CTR':'{0:.2%}','ads value增长值':'{0:.2%}','ads ROI增长值':'{0:.2%}','click增长值':'{0:.2%}','自选日期花费占比':'{0:.2%}',
    '对比日期花费占比':'{0:.2%}','自选日期ads value占比':'{0:.2%}','对比日期ads value占比':'{0:.2%}','自选日期CVR':'{0:.2%}','对比日期CVR':'{0:.2%}','CVR增长值':'{0:.2%}',
    'Sale Price':'{0:.2f}','自选日期花费':'{0:.2f}','对比日期花费':'{0:.2f}','自选日期CPC':'{0:.2f}','对比日期CPC':'{0:.2f}','自选日期ads value':'{0:.2f}','对比日期ads value':'{0:.2f}',
    '自选日期该三级类目神策ROI':'{0:.2f}','对比日期该三级类目神策ROI':'{0:.2f}'
}
column_config={"imagelink": st.column_config.ImageColumn(width="small")}
style_mapping = {
    'CPC增长值': 'RdPu',
    'cost增长值': 'OrRd',
    'ads value增长值': 'viridis',
    'conversions增长值':'YlGn',
    'ads ROI增长值':'YlOrRd',
    'CTR增长值':'Blues',
     'CVR增长值':'YlOrRd',
}
# 根据options创建新的style_mapping_match
style_mapping_match = {}
for option in category_3_compare_select_options:
    if option in style_mapping:
        style_mapping_match[option] = style_mapping[option]

styled_df_compare = remove_category_3_summary_df[category_3_compare_select_options].style\
    # .set_index(['SKU','imagelink'])

for column, cmap in style_mapping_match.items():
    styled_df_compare = styled_df_compare.background_gradient(subset=[column], cmap=cmap)

st.dataframe(styled_df_compare.format(format_dict),height=800, width=2600,column_config=column_config,hide_index=True)
with st.container():
    col1, col2= st.columns(2)
    with col1:
      st.subheader('自选日期三级类目花费趋势')
      st.line_chart(ads_daily_filter_category_3_trend_df,x="Date", y="cost")
    with col2:
      st.subheader('对比日期三级类目花费趋势')
      st.line_chart(compare_ads_daily_filter_category_3_trend_df,x="Date", y="cost")

st.subheader('三级类目自选时间范围数据')
category_3_select_options = st.multiselect(
    '数据维度',
    remove_category_3_summary_df.columns,
    ['SKU', 'imagelink', 'old_or_new', '自选日期花费', '自选日期ads value','自选日期ads-ROI','自选日期内该三级类目平均ads-ROI','自选日期该三级类目神策ROI','自选日期CPC',
     '自选日期CTR','自选日期CVR','自选日期花费占比','自选日期ads value占比']
)
style_mapping_match_select = {}
for option in category_3_select_options:
    if option in style_mapping:
        style_mapping_match_select[option] = style_mapping[option]
styled_df = remove_category_3_summary_df[category_3_select_options].style
for column, cmap in style_mapping_match_select.items():
    styled_df = styled_df.background_gradient(subset=[column], cmap=cmap)

st.dataframe(styled_df.format(format_dict),height=800, width=2600,column_config=column_config,hide_index=True)
