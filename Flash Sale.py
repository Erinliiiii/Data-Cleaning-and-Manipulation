#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from pandasql import sqldf

######################################
#### 1. Update path of clean data ####
######################################
# load cleaned data
cleaned = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\July21\CleanedData.xlsx', 
                        sheet_name='CM to GS (Ready)')

cleaned2 = cleaned.copy()
cleaned2['lob'] = cleaned2['LineOfBusinessType']

# creating a column indicating short name
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Gene Synthesis', 'lob'] = 'gs'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Molecular Genetics', 'lob'] = 'molgen'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Next Gen. Sequencing', 'lob'] = 'ngs'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Oligo Synthesis', 'lob'] = 'oligo'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Plasmid DNA Prep.', 'lob'] = 'prep'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Regulatory', 'lob'] = 'rs'
cleaned2.loc[cleaned2['LineOfBusinessType'] == 'Sanger Sequencing', 'lob'] = 'sanger'

# concat email and LoB
cleaned2['email_lob'] = cleaned2['CustomerEmail'] + '*' + cleaned2['lob']
# lowercase all the email address
cleaned2['email_lob'] = cleaned2['email_lob'].str.lower()
cleaned2['CustomerEmail'] = cleaned2['CustomerEmail'].str.lower()


###########################################
#### 2. Update path of historical list ####
###########################################
# load historical customer list
historical = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\July\Historical Customer List for Campaign Tracking_updated.xlsx',
                          sheet_name = 'No 2019')

# change column name
historical.columns = [c.lower().replace(' ', '_') for c in historical.columns]
historical.rename(columns={'email':'CustomerEmail','email_+_lob':'email_lob'},inplace=True)

########################################################################################
#### 3. Update path and file name of flash email open lists; Update email sent date ####
########################################################################################
flash_ex_gs_open = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\hubspot_list\Flash Sale 2019 (Exclude GS).xlsx')
flash_cl_gs_open = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\hubspot_list\Flash Sale 2019 (Clone to GS).xlsx')

# convert column name to lower case and replace space with '_'
flash_ex_gs_open.columns = [c.lower().replace(' ', '_') for c in flash_ex_gs_open.columns]
flash_cl_gs_open.columns = [c.lower().replace(' ', '_') for c in flash_cl_gs_open.columns]

# keep records with date after 1/3/2019
raw_flash = cleaned2.loc[cleaned2['CreatedDate'] >= '2019-01-03 00:00:00', :]

# keep records with "FLASH" promo code
raw_flash.loc[:,'PromotionCode'] = raw_flash['PromotionCode'].str.lower()
ready_flash = raw_flash.loc[raw_flash.PromotionCode.str.contains('flash', na = False), :]


# 1. Inner join 'Flash exclude GS' and 'Flash clone to GS' with SalesDashboard separately
# 2. Label them with email name
# 3. Combine two tables

# flash exclude GS
# lowercase the email address in email open list
flash_ex_gs_open['recipient'] = flash_ex_gs_open['recipient'].str.lower()

# inner join raw_flash and flash exclude GS list
flash_ex_gs_labeled = pd.merge(left=ready_flash,
                            right=flash_ex_gs_open[['recipient']],
                            left_on='CustomerEmail',
                            right_on='recipient',
                            how='inner').drop(columns=['recipient'])

# add a column indicating labeled email
flash_ex_gs_labeled['recent_open'] = 'flash exclude gs'


# flash clone to GS
# lowercase the email address
flash_cl_gs_open['recipient'] = flash_cl_gs_open['recipient'].str.lower()

# inner join raw_flash and flash clone to GS list
flash_cl_gs_labeled = pd.merge(left=ready_flash,
                            right=flash_cl_gs_open[['recipient']],
                            left_on='CustomerEmail',
                            right_on='recipient',
                            how='inner').drop(columns=['recipient'])

# add a column indicating labeled email
flash_cl_gs_labeled['recent_open'] = 'flash clone to gs'

# concat two labeled table
flash_labeled = pd.concat([flash_ex_gs_labeled, flash_cl_gs_labeled], ignore_index = True, sort = False)


# #### Add a column indicating if is a quote-based order
flash_labeled.loc[flash_labeled['LineOfBusinessType'].isin(['Gene Synthesis', 'Next Gen. Sequencing', 'Molecular Genetics', 'Regulatory']),'quote_based'] = 1
# convert 'quote_based' value from NA to 0 if is not quote_based
flash_labeled['quote_based'] = flash_labeled.loc[:, 'quote_based'].fillna(0)

# #### Add a column indicating if is confirmed
# two conditions:
# if is quote_based, and order status is 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'non_confirmed' 
flash_labeled.loc[(flash_labeled['quote_based']==1)&(flash_labeled['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'non_confirmed'
# if is quote_based, and order status is not 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'confirmed' 
flash_labeled.loc[(flash_labeled['quote_based']==1)&(~flash_labeled['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'confirmed'
# for direct order, svalue for column 'confirmed' should be set as 'confirmed'
flash_labeled.loc[flash_labeled['quote_based']==0, 'confirmed'] = 'confirmed'

#######################################################################################
#### 4. Update sent out date for flash sales email when making the historical list ####
#######################################################################################
# get the order list from 1/1/2019 to 1/2/2019
jan1_jan2 = cleaned2 .loc[cleaned2['CreatedDate'] < '2019-01-03 00:00:00', :]

# concat customer list of 2016-2018 and list of 2019
hist_list_flash = pd.concat([historical[['CustomerEmail', 'email_lob']],jan1_jan2[['CustomerEmail', 'email_lob']]], 
                       ignore_index=True, sort =False)

# change the column name for joining 
hist_list_flash.rename(columns={'CustomerEmail':'email', 'email_lob':'email_and_lob'}, inplace=True)
hist_list_flash['email'] = hist_list_flash['email'].str.lower()
hist_list_flash['email_and_lob'] = hist_list_flash['email_and_lob'].str.lower()

# keep unique email in historial list to check if a custsomer is new 
hist_list_flash_email_de = hist_list_flash.drop_duplicates(subset = 'email', keep = 'first')
# keep unique email+LoB in historial list to check if a custsomer is new to LoB
hist_list_flash_lob_de = hist_list_flash.drop_duplicates(subset = 'email_and_lob', keep = 'first')


# #### If new to GWZ

# left join flash_labeled and historical email list based on email
flash_labeled = pd.merge(left=flash_labeled, 
                            right=hist_list_flash_email_de[['email']],
                            left_on='CustomerEmail',
                            right_on='email',
                            how='left')

# add a column indicating if it's new to company
flash_labeled['new_to_company'] = (flash_labeled.email.isna()).astype(np.int)


# #### If new to LoB

# left join flash_labeled and historical email+lob list based on email+lob
flash_labeled = pd.merge(left=flash_labeled, 
                            right=hist_list_flash_lob_de[['email_and_lob']],
                            left_on='email_lob',
                            right_on='email_and_lob',
                            how='left')

# add a column indicating if it's new to lob
flash_labeled['new_to_lob'] = (flash_labeled.email_and_lob.isna()).astype(np.int)

# for new to company customers, convert value of new_to_lob to NA
flash_labeled.loc[flash_labeled['new_to_company'] == 1, 'new_to_lob'] = np.nan


flash_final = flash_labeled.copy()

email_short_name = list(['flash exclude gs', 'flash clone to gs'])


# # Flash Metrics

def get_metrics(final_data, index_order):
    final = final_data.copy()
    
    # quote-based orders
    quote_q = """
    SELECT recent_open,
            COUNT(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'non_confirmed' THEN CustomerEmail ELSE NULL END) AS NGS_NonConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS NGS_NonConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'confirmed' THEN CustomerEmail ELSE NULL END) AS NGS_ConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS NGS_ConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'non_confirmed' THEN CustomerEmail ELSE NULL END) AS GS_NonConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS GS_NonConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'confirmed' THEN CustomerEmail ELSE NULL END) AS GS_ConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS GS_ConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'non_confirmed' THEN CustomerEmail ELSE NULL END) AS Molgen_NonConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS Molgen_NonConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'confirmed' THEN CustomerEmail ELSE NULL END) AS Molgen_ConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS Molgen_ConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'non_confirmed' THEN CustomerEmail ELSE NULL END) AS RS_NonConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS RS_NonConfirmedRevenue,
            COUNT(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'confirmed' THEN CustomerEmail ELSE NULL END) AS RS_ConfirmedNumber,
            SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS RS_ConfirmedRevenue
    FROM final
    GROUP BY recent_open;
    """
    quote = sqldf(quote_q, locals())
    # reindex the table to make it ordered by email sent date
    quote.set_index('recent_open', inplace=True)
    quote = quote.reindex(index_order)
    
    # direct orders
    direct_q = """
                SELECT recent_open,
                    COUNT(CASE WHEN LineOfBusinessType = 'Sanger Sequencing' THEN CustomerEmail ELSE NULL END) AS Sanger_Order,
                    SUM(CASE WHEN LineOfBusinessType = 'Sanger Sequencing' THEN BillingAmount ELSE 0 END) AS Sanger_Revenue,
                    COUNT(CASE WHEN LineOfBusinessType = 'Plasmid DNA Prep.' THEN CustomerEmail ELSE NULL END) AS Prep_Order,
                    SUM(CASE WHEN LineOfBusinessType = 'Plasmid DNA Prep.' THEN BillingAmount ELSE 0 END) AS Prep_Revenue,
                    COUNT(CASE WHEN LineOfBusinessType = 'Oligo Synthesis' THEN CustomerEmail ELSE NULL END) AS Oligo_Order,
                    SUM(CASE WHEN LineOfBusinessType = 'Oligo Synthesis' THEN BillingAmount ELSE 0 END) AS Oligo_Revenue
                FROM final
                GROUP BY recent_open;
                """
    direct = sqldf(direct_q, locals())
    # reindex the table to make it ordered by email sent date
    direct.set_index('recent_open', inplace=True)
    direct = direct.reindex(index_order)
    
    # metrics by customer status
    customer_status_q = """
    SELECT recent_open,
            COUNT(CASE WHEN new_to_company = 0 THEN CustomerEmail ELSE NULL END) AS Return_Order,
            SUM(CASE WHEN new_to_company = 0 THEN BillingAmount ELSE 0 END) AS Return_Revenue,
            COUNT(CASE WHEN new_to_company = 1 THEN CustomerEmail ELSE NULL END) AS NewToGWZ_Order,
            SUM(CASE WHEN new_to_company = 1 THEN BillingAmount ELSE 0 END) AS NewToGWZ_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'ngs' THEN CustomerEmail ELSE NULL END) AS NewToNGS_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'ngs' THEN BillingAmount ELSE 0 END) AS NewToNGS_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'gs' THEN CustomerEmail ELSE NULL END) AS NewToGS_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'gs' THEN BillingAmount ELSE 0 END) AS NewToGS_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'molgen' THEN CustomerEmail ELSE NULL END) AS NewToMolGen_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'molgen' THEN BillingAmount ELSE 0 END) AS NewToMolGen_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'rs' THEN CustomerEmail ELSE NULL END) AS NewToRS_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'rs' THEN BillingAmount ELSE 0 END) AS NewToRS_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'sanger' THEN CustomerEmail ELSE NULL END) AS NewToSanger_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'sanger' THEN BillingAmount ELSE 0 END) AS NewToSanger_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'prep' THEN CustomerEmail ELSE NULL END) AS NewToPrep_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'prep' THEN BillingAmount ELSE 0 END) AS NewToPrep_Revenue,
            COUNT(CASE WHEN new_to_lob = 1 AND lob = 'oligo' THEN CustomerEmail ELSE NULL END) AS NewToOligo_Order,
            SUM(CASE WHEN new_to_lob = 1 AND lob = 'oligo' THEN BillingAmount ELSE 0 END) AS NewToOligo_Revenue
    FROM final
    GROUP BY recent_open;
    """

    pd.set_option('display.max_columns', 500)
    customer_status = sqldf(customer_status_q, locals())
    # reindex the table to make it ordered by email sent date
    customer_status.set_index('recent_open', inplace=True)
    customer_status = customer_status.reindex(index_order)
    
    # number of new customer acquired
    new_customer_acquired_q = """
    SELECT recent_open,
            COUNT (DISTINCT(CASE WHEN new_to_company = 1 THEN CustomerEmail ELSE NULL END)) AS NewToGWZ,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'ngs' THEN CustomerEmail ELSE NULL END)) AS NewToNGS,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'gs' THEN CustomerEmail ELSE NULL END)) AS NewToGS,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'molgen' THEN CustomerEmail ELSE NULL END)) AS NewToMolGen,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'rs' THEN CustomerEmail ELSE NULL END)) AS NewToRS,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'sanger' THEN CustomerEmail ELSE NULL END)) AS NewToSanger,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'prep' THEN CustomerEmail ELSE NULL END)) AS NewToPrep,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 AND lob = 'oligo' THEN CustomerEmail ELSE NULL END)) AS NewToOligo
    FROM final
    GROUP BY recent_open;
    """
    new_customer_acquired = sqldf(new_customer_acquired_q, locals())
    # reindex the table to make it ordered by email sent date
    new_customer_acquired.set_index('recent_open', inplace=True)
    new_customer_acquired = new_customer_acquired.reindex(index_order)
    
    # number of unique customers
    unique_number_q = """
                    SELECT recent_open,
                    COUNT(DISTINCT(CustomerEmail)) AS '# of unique customers'
                    FROM final
                    GROUP BY recent_open;
                """
    unique_number = sqldf(unique_number_q, locals())
    # reindex the table to make it ordered by email sent date
    unique_number.set_index('recent_open', inplace=True)
    unique_number = unique_number.reindex(index_order)
    
    # combine all the metrics horizontally
    metrics = pd.concat([quote, direct, customer_status, new_customer_acquired, unique_number], axis = 1)
    
    return metrics

# call the get_metrics function
flash_metrics = get_metrics(flash_final, email_short_name)

# # Flash sales revenue breakdown

# ## All sources

# ready_flash is the table which includes orders with Flash Code
ready_flash.loc[ready_flash['LineOfBusinessType'].isin(['Gene Synthesis', 'Next Gen. Sequencing', 'Molecular Genetics', 'Regulatory']),'quote_based'] = 1
# convert 'quote_based' value from NA to 0 if is not quote_based
ready_flash.loc[:, 'quote_based'] = ready_flash.loc[:, 'quote_based'].fillna(0)

# add a 'confirmed' column to ready_flash table
# two conditions:
# if is quote_based, and order status is 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'non_confirmed' 
ready_flash.loc[(ready_flash['quote_based']==1)&(ready_flash['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'non_confirmed'
# if is quote_based, and order status is not 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'confirmed' 
ready_flash.loc[(ready_flash['quote_based']==1)&(~ready_flash['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'confirmed'
# for direct order, svalue for column 'confirmed' should be set as 'confirmed'
ready_flash.loc[ready_flash['quote_based']==0, 'confirmed'] = 'confirmed'


flash_all_source_q = """
    SELECT 
    (NGS_NonConfirmedRevenue + NGS_ConfirmedRevenue) AS NGS_Total, NGS_NonConfirmedRevenue, NGS_ConfirmedRevenue, 
    (GS_NonConfirmedRevenue + GS_ConfirmedRevenue) AS GS_Total, GS_NonConfirmedRevenue, GS_ConfirmedRevenue, 
    (MolGen_NonConfirmedRevenue + MolGen_ConfirmedRevenue) AS MolGen_Total, MolGen_NonConfirmedRevenue, MolGen_ConfirmedRevenue,
    (RS_NonConfirmedRevenue + RS_ConfirmedRevenue) AS RS_Total, RS_NonConfirmedRevenue, RS_ConfirmedRevenue,
    Sanger_Revenue, Prep_Revenue, Oligo_Revenue
    FROM(
    SELECT
    SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS NGS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS NGS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS GS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS GS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS MolGen_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS MolGen_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS RS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS RS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Sanger Sequencing' THEN BillingAmount ELSE 0 END) AS Sanger_Revenue,
    SUM(CASE WHEN LineOfBusinessType = 'Plasmid DNA Prep.' THEN BillingAmount ELSE 0 END) AS Prep_Revenue,
    SUM(CASE WHEN LineOfBusinessType = 'Oligo Synthesis' THEN BillingAmount ELSE 0 END) AS Oligo_Revenue
    FROM ready_flash
    ) sub;
    """

flash_all_source = sqldf(flash_all_source_q, locals())

# Convert table from horizontal to vertical
flash_all_source = flash_all_source.T
# rename the column name as 'All Sources'
flash_all_source = flash_all_source.rename(columns={0: 'All Sources'})

# ## Open Email

flash_open_q = """
    SELECT 
    (NGS_NonConfirmedRevenue + NGS_ConfirmedRevenue) AS NGS_Total, NGS_NonConfirmedRevenue, NGS_ConfirmedRevenue, 
    (GS_NonConfirmedRevenue + GS_ConfirmedRevenue) AS GS_Total, GS_NonConfirmedRevenue, GS_ConfirmedRevenue, 
    (MolGen_NonConfirmedRevenue + MolGen_ConfirmedRevenue) AS MolGen_Total, MolGen_NonConfirmedRevenue, MolGen_ConfirmedRevenue,
    (RS_NonConfirmedRevenue + RS_ConfirmedRevenue) AS RS_Total, RS_NonConfirmedRevenue, RS_ConfirmedRevenue,
    Sanger_Revenue, Prep_Revenue, Oligo_Revenue
    FROM(
    SELECT
    SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS NGS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS NGS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS GS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS GS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS MolGen_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS MolGen_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS RS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS RS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Sanger Sequencing' THEN BillingAmount ELSE 0 END) AS Sanger_Revenue,
    SUM(CASE WHEN LineOfBusinessType = 'Plasmid DNA Prep.' THEN BillingAmount ELSE 0 END) AS Prep_Revenue,
    SUM(CASE WHEN LineOfBusinessType = 'Oligo Synthesis' THEN BillingAmount ELSE 0 END) AS Oligo_Revenue
    FROM flash_final
    ) sub;
    """

flash_open = sqldf(flash_open_q, locals())
# Convert table from horizontal to vertical
flash_open = flash_open.T
# rename the column name as 'Open Email'
flash_open = flash_open.rename(columns={0: 'Open Email'})


# ## Open & Click

# get click lists
flash_ex_gs_click = flash_ex_gs_open.loc[flash_ex_gs_open['clicked']==True, :]
flash_cl_gs_click = flash_cl_gs_open.loc[flash_cl_gs_open['clicked']==True, :]

# inner join two click lists with flash orders
flash_ex_gs_open_click = pd.merge(left=ready_flash,
                            right=flash_ex_gs_click[['recipient']],
                            left_on='CustomerEmail',
                            right_on='recipient',
                            how='inner').drop(columns=['recipient'])

flash_cl_gs_open_click = pd.merge(left=ready_flash,
                            right=flash_cl_gs_click[['recipient']],
                            left_on='CustomerEmail',
                            right_on='recipient',
                            how='inner').drop(columns=['recipient'])

# concat those two tables
flash_click = pd.concat([flash_ex_gs_open_click, flash_cl_gs_open_click], ignore_index = True, sort = False)

# add a 'confirmed' column to flash_click table
# two conditions:
# if is quote_based, and order status is 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'non_confirmed' 
flash_click.loc[(flash_click['quote_based']==1)&(flash_click['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'non_confirmed'
# if is quote_based, and order status is not 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'confirmed' 
flash_click.loc[(flash_click['quote_based']==1)&(~flash_click['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'confirmed'
# for direct order, svalue for column 'confirmed' should be set as 'confirmed'
flash_click.loc[flash_click['quote_based']==0, 'confirmed'] = 'confirmed'

flash_open_click_q = """
    SELECT 
    (NGS_NonConfirmedRevenue + NGS_ConfirmedRevenue) AS NGS_Total, NGS_NonConfirmedRevenue, NGS_ConfirmedRevenue, 
    (GS_NonConfirmedRevenue + GS_ConfirmedRevenue) AS GS_Total, GS_NonConfirmedRevenue, GS_ConfirmedRevenue, 
    (MolGen_NonConfirmedRevenue + MolGen_ConfirmedRevenue) AS MolGen_Total, MolGen_NonConfirmedRevenue, MolGen_ConfirmedRevenue,
    (RS_NonConfirmedRevenue + RS_ConfirmedRevenue) AS RS_Total, RS_NonConfirmedRevenue, RS_ConfirmedRevenue,
    Sanger_Revenue, Prep_Revenue, Oligo_Revenue
    FROM(
    SELECT
    SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS NGS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Next Gen. Sequencing' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS NGS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS GS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Gene Synthesis' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS GS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS MolGen_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Molecular Genetics' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS MolGen_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS RS_NonConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Regulatory' AND confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS RS_ConfirmedRevenue,
    SUM(CASE WHEN LineOfBusinessType = 'Sanger Sequencing' THEN BillingAmount ELSE 0 END) AS Sanger_Revenue,
    SUM(CASE WHEN LineOfBusinessType = 'Plasmid DNA Prep.' THEN BillingAmount ELSE 0 END) AS Prep_Revenue,
    SUM(CASE WHEN LineOfBusinessType = 'Oligo Synthesis' THEN BillingAmount ELSE 0 END) AS Oligo_Revenue
    FROM flash_click
    ) sub;
    """

flash_open_click = sqldf(flash_open_click_q, locals())
# Convert table from horizontal to vertical
flash_open_click = flash_open_click.T
# rename the column name as 'Open & Click'
flash_open_click = flash_open_click.rename(columns={0: 'Open & Click'})

# Combine above three tables
flash_breakdown = pd.concat([flash_all_source, flash_open, flash_open_click], axis = 1)


# # New customer list

# new to company
flash_new_to_company_list = flash_final.loc[flash_final['new_to_company'] == 1, ['CustomerEmail', 'UserName', 'Institution', 'recent_open', 'lob']]
flash_new_to_company_list.loc[:, 'recent_open'] = 'Flash Sale 2019'
flash_new_to_company_list.insert(loc=0, column='New Customer Type', value='New to GENEWIZ')

# new to lob
flash_new_to_lob_list = flash_final.loc[flash_final['new_to_lob']==1,
                                          ['CustomerEmail', 'UserName', 'Institution', 'recent_open', 'lob']]
flash_new_to_lob_list.loc[:, 'recent_open'] = 'Flash Sale 2019'
flash_new_to_lob_list.insert(loc=0, column='New Customer Type', value='New to ' + flash_new_to_lob_list.lob)
flash_new_to_lob_list.loc[:,'lob'] = 'N/A'

new_customer_list_complete = pd.concat([flash_new_to_company_list, flash_new_to_lob_list], 
                                       ignore_index = True, sort = False)

new_customer_list_complete = new_customer_list_complete.rename(columns={'CustomerEmail':'Customer Email', 'UserName':'Customer Name',
                                                                        'combined_email':'Campaign Source', 'lob':'First Order LoB'})
# deduplicate list
new_customer_list_complete = new_customer_list_complete.drop_duplicates(subset = None, keep = 'first')

###################################################################
#### 5. Update path and file name before exporting the results ####
###################################################################

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\Marketing Campaign Tracking Python\July21\Flash\open_allSources_openClick.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
flash_final.to_excel(writer, sheet_name='Flash Open Email', index=False)
ready_flash.to_excel(writer, sheet_name='Flash All Sources', index=False)
flash_open_click.to_excel(writer, sheet_name='Flash Open & Click', index=False)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\Marketing Campaign Tracking Python\July21\Flash\Flash Metrics.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
flash_metrics.to_excel(writer, sheet_name='Flash Metrics', index=True)
flash_breakdown.to_excel(writer, sheet_name='Flash Breakdown', index=True)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

new_customer_list_complete.to_excel(r'Z:\Marketing Campaign Tracking Python\July21\Flash\new_customer_list_july21.xlsx', index=False)

