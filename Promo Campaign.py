#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from pandasql import sqldf


# Import clean data and historical list

######################################
#### 1. Update path of clean data ####
######################################
# load cleaned data
cleaned = pd.read_excel(r'Z:\August13\CleanedData.xlsx', 
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
historical = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\July21\Historical Customer List for Campaign Tracking_updated.xlsx',
                          sheet_name = 'No 2019 H2')

# change column name
historical.columns = [c.lower().replace(' ', '_') for c in historical.columns]
historical.rename(columns={'email':'CustomerEmail','email_+_lob':'email_lob'},inplace=True)


# # Import index table

# Create an index table:
# 1. 'long_name': stands for the file names of all email open lists
# 2. 'short_name': stands for the short names of all emails
# 3. 'sent_date': stands for the sent dates of all emails
# 4. order types

#######################################
#### 3. Update path of index table ####
#######################################
index_table = pd.read_excel(r'Z:\August13\Promo\index_table.xlsx')

# convert sent_date to datetime type
index_table['sent_date'] = pd.to_datetime(index_table['sent_date'])
# if is checked, then 1; if is not, then 0.
index_table.iloc[:, 3:] = (index_table.iloc[:, 3:].notna()).astype(np.int)
# make sure that email_name in index_table is the same as the name of excel file
# remove '/' in email_name in index_table, since name for excel file cannot contain '/'
index_table['long_name'] = index_table.long_name.apply(lambda x: x.replace('/', ''))

################################################################################
#### 4. Update the criteria for filtering the order: keep NGS, MolGen, RS; ####
#### for GS, here we only keep 'Priority GENE'  ################################
################################################################################
# Keep all NGS and MolGen
ngs_molgen_rs = cleaned2.loc[cleaned2['LineOfBusinessType'].isin(['Next Gen. Sequencing', 'Molecular Genetics',
                                                                'Regulatory']), :]
# For GS, only keep Priority GENE order
gs = cleaned2.loc[(cleaned2['LineOfBusinessType']=='Gene Synthesis')&(cleaned2['Priority']=='PriorityGENE'),:]

# combine all LoB
ready = pd.concat([ngs_molgen_rs, gs])

# create a list of all ngs email name
all_long_name = list(index_table.long_name)

# # Add all email columns to sales dashboard

#######################################################################
#### 5. Update path of promo campaign email open lists in function ####
#######################################################################

def add_open_list(email_long_name, ready_data, index_table):
    # import all email open lists
    path = r'Z:\August13\Promo'
    email_opened = pd.read_excel(path + '\\' + email_long_name + ".xlsx")
    
    # convert column name to lower case and replace space with '_'
    email_opened.columns = [c.lower().replace(' ', '_') for c in email_opened.columns]
    
    # get the corresponding short name to that email based on index_table
    email_short_name = index_table.loc[index_table['long_name']==email_long_name,'short_name'].values[0]
    
    # add the email sent out date to email list
    email_opened[email_short_name]=email_opened['sent_at_(your_time_zone)']
    email_opened=email_opened[[email_short_name,'recipient']]
    
    # lowercase the email address
    email_opened['recipient'] = email_opened['recipient'].str.lower()
    
    # Add email column to raw dataset
    add_open=pd.merge(left=ready_data,right=email_opened,left_on='CustomerEmail',right_on='recipient',
                             how='left').drop(columns=['recipient'])
    

    return add_open


# loop add_open_list function throught all the ngs promo email open list.
for i in range(len(all_long_name)):
    ready = add_open_list(all_long_name[i], ready, index_table)


# # Label the email to each order

labeled = ready.copy()

all_short_name=list(labeled.columns)[28:]

# drop rows with no email opened
labeled = labeled.dropna(subset=all_short_name, how='all')
# Convert NaNs to 2020-01-29 00:00:00
labeled[all_short_name] = labeled.iloc[:,28:].fillna(pd.to_datetime('2020-01-31 00:00:00')) # the date could be set as a later date if necessary
labeled[all_short_name] = labeled.iloc[:,28:].apply(lambda x:pd.to_datetime(x,format = '%Y-%m-%d %H:%M:%S'))
labeled[all_short_name] = labeled.iloc[:,28:].apply(lambda x:x-labeled['CreatedDate'])

# ### Subset by order type then label relevant email to each order

def find_recent(x, email_name):
    # get the max negative number in a row
    small_number = pd.Timedelta(days=-99999);
    for i in range(len(email_name)):
        if x[i]<pd.Timedelta(0):
            small_number = max(small_number, x[i])
            
    if small_number == pd.Timedelta(days=-99999):
        return 'none_open'

    # return the index of the item has that value
    for i in email_name:
        if x[i]==small_number:
            return i


# ### Write a function to label the email to a specific order type then loop the function through all the order type

def select_order_type(order_type_name):
    relevant_email = list(index_table.loc[index_table[order_type_name]==1,'short_name'])
    labeled.loc[labeled['OrderType'] == order_type_name,'recent_open'] = labeled[relevant_email].apply(lambda x:find_recent(x, relevant_email),1)
    return labeled

# create a list of all order type
all_order_type = index_table.columns[3:]

# apply the select order function to all order types
for i in range(len(all_order_type)):
    select_order_type(all_order_type[i])

###########################################################
#### 6. Update path tp export complete sales dashboard ####
#### including none_open and order type we don't track ####
###########################################################
# export table with all records in sales dashboard, including orders with no open email
labeled.to_excel(r'Z:\August13\Promo\Promo_labeled_complete.xlsx', index=False)
# Drop customers who have not opened any emails before they placed an order
labeled = labeled[labeled.recent_open != 'none_open']
# Drop recent_open which is NA, those are the order types that we do not want to track
labeled = labeled.dropna(subset = ['recent_open'])


# #### Add a column indicating if is non_confirmed

# if order status is 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'non_confirmed' 
labeled.loc[labeled['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order']),'confirmed'] = 'non_confirmed'
# if order status is not 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'confirmed' 
labeled.loc[~labeled['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order']),'confirmed'] = 'confirmed'


# #### If new to GWZ

# create an empty dataframe so that we can append seperated table from each email into it one by one
new_to_gwz = pd.DataFrame()

def check_new_to_gwz(email_labeled_table, email_short_name, index_table, empty_df):
    # get the corresponding date for the selected email short name
    date = index_table.loc[index_table['short_name'] == email_short_name, 'sent_date'].values[0]
    
    # create the historical list
    his_list_2019 = cleaned2[cleaned2['CreatedDate'] < date]
    his_list = pd.concat([historical[['CustomerEmail', 'email_lob']],his_list_2019[['CustomerEmail', 'email_lob']]], 
                           ignore_index=True, sort =False)
    
    # change the column name for joining, and lowercase email address 
    his_list.rename(columns={'CustomerEmail':'email', 'email_lob':'email_and_lob'}, inplace=True)
    his_list['email'] = his_list['email'].str.lower()
    his_list['email_and_lob'] = his_list['email_and_lob'].str.lower()

    # keep unique email in historial list for checking if a custsomer is new 
    list_email_de = his_list.drop_duplicates(subset = 'email', keep = 'first')
    
    # keep records labeled specific email name
    tmp_labeled_email = email_labeled_table.loc[email_labeled_table['recent_open'] == email_short_name, :]

    # left join
    tmp = pd.merge(left=tmp_labeled_email,
                   right=list_email_de[['email']],
                   left_on='CustomerEmail',
                   right_on='email',
                   how='left')

    # add a column indicating if it's a new customer
    tmp['new_to_company'] = (tmp.email.isna()).astype(np.int)
    
    # append the result to empty dataframe
    appended = empty_df.append(tmp)
    
    return appended

# loop through all the email name
for i in range(len(all_short_name)):
    new_to_gwz = check_new_to_gwz(labeled, all_short_name[i], index_table, new_to_gwz)


# #### If new to LoB

# create an empty dataframe so that we can append seperate table from each email into it one by one
new_to_lob = pd.DataFrame()

def check_new_to_lob(email_new_to_gwz_added_table, email_short_name, index_table, empty_df):
    # get the corresponding date for the selected email short name
    date = index_table.loc[index_table['short_name'] == email_short_name, 'sent_date'].values[0]
    
    # create the historical list
    his_list_2019 = cleaned2[cleaned2['CreatedDate'] < date]
    his_list = pd.concat([historical[['CustomerEmail', 'email_lob']],his_list_2019[['CustomerEmail', 'email_lob']]], 
                           ignore_index=True, sort =False)
    
    # change the column name for joining, and lowercase email address 
    his_list.rename(columns={'CustomerEmail':'email', 'email_lob':'email_and_lob'}, inplace=True)
    his_list['email'] = his_list['email'].str.lower()
    his_list['email_and_lob'] = his_list['email_and_lob'].str.lower()

    # keep unique email+LoB in historial list for checking if a custsomer is new to LoB
    list_email_lob_de = his_list.drop_duplicates(subset = 'email_and_lob', keep = 'first')
    
    # keep records labeled specific email name
    tmp_labeled_email = email_new_to_gwz_added_table.loc[email_new_to_gwz_added_table['recent_open'] == email_short_name, :]

    # left join
    tmp = pd.merge(left=tmp_labeled_email,
                   right=list_email_lob_de[['email_and_lob']],
                   left_on='email_lob',
                   right_on='email_and_lob',
                   how='left')

    # add a column indicating if it's a new customer
    tmp['new_to_lob'] = (tmp.email_and_lob.isna()).astype(np.int)
    
    # append the result to empty dataframe
    appended = empty_df.append(tmp)
    
    return appended

# loop through all the email name
for i in range(len(all_short_name)):
    new_to_lob = check_new_to_lob(new_to_gwz, all_short_name[i], index_table, new_to_lob)

# for returning customers, convert their value of new_to_lob to Na
new_to_lob.loc[new_to_lob['new_to_company'] == 1, 'new_to_lob'] = np.nan


final = new_to_lob.copy()

############################################
#### 7. Update the combination criteria ####
############################################

# add a column indicating combined email name
final.loc[final['recent_open'].isin(['Jul NGS WGS Promo (Onco & Anti)', 'Jul NGS WGS Promo (Micro & Others)']), 'combined_email'] = 'Jul NGS WGS Promo'
final.loc[final['recent_open'].isin(['Aug NGS Pick Your Promo (Onco & Others)']), 'combined_email'] = 'Aug NGS Pick Your Promo'
final.loc[final['recent_open'].isin(['Jul GS PriorityGENE (Micro & Others)', 'Jul GS PriorityGENE (Onco & Others)', 
                                    'Jul GS off PriorityGENE (MiSeq Leftovers)']), 'combined_email'] = 'Jul GS PriorityGENE'
final.loc[final['recent_open'].isin(['Jul MolGen/RS Open Quotes Promo']), 'combined_email'] = 'Jul MolGen/RS Open Quotes Promo'
final.loc[final['recent_open'].isin(['Aug MolGen/RS 30% off PCR+Sanger']), 'combined_email'] = 'Aug MolGen/RS 30% off PCR+Sanger'


# ## Metrics Calculation

# #### Revenue by confirmed

def get_promo_metrics(final_data, index_order):
    final = final_data.copy()
    
    # revenue by confirmed
    confirmed_q = """
    SELECT recent_open,
            COUNT(CASE WHEN confirmed = 'non_confirmed' THEN CustomerEmail ELSE NULL END) AS NonConfirmedOrder,
            SUM(CASE WHEN confirmed = 'non_confirmed' THEN BillingAmount ELSE 0 END) AS NonConfirmedRevenue,
            COUNT(CASE WHEN confirmed = 'confirmed' THEN CustomerEmail ELSE NULL END) AS ConfirmedOrder,
            SUM(CASE WHEN confirmed = 'confirmed' THEN BillingAmount ELSE 0 END) AS ConfirmedRevenue
    FROM final
    GROUP BY recent_open;
    """
    by_confirmed = sqldf(confirmed_q, locals())
    # reindex the table to make it ordered by email sent date
    by_confirmed.set_index('recent_open', inplace=True)
    by_confirmed = by_confirmed.reindex(index_order)
    
    # revenue by customer status
    customer_status_q = """
    SELECT recent_open,
           COUNT(CASE WHEN new_to_company = 0 THEN CustomerEmail ELSE NULL END) AS Return_Order,
           SUM(CASE WHEN new_to_company = 0 THEN BillingAmount ELSE 0 END) AS Return_Revenue,
           COUNT(CASE WHEN new_to_company = 1 THEN CustomerEmail ELSE NULL END) AS NewToGWZ_Order,
           SUM(CASE WHEN new_to_company = 1 THEN BillingAmount ELSE 0 END) AS NewToGWZ_Revenue,
           COUNT(CASE WHEN new_to_lob = 1 THEN CustomerEmail ELSE NULL END) AS NewToLoB_Order,
           SUM(CASE WHEN new_to_lob = 1 THEN BillingAmount ELSE 0 END) AS NewToLoB_Revenue
    FROM final
    GROUP BY recent_open;
    """
    by_customer_status = sqldf(customer_status_q, locals())
    # reindex the table to make it ordered by email sent date
    by_customer_status.set_index('recent_open', inplace=True)
    by_customer_status = by_customer_status.reindex(index_order)
    
    # new customer acquired
    new_customer_acquired_q = """
    SELECT recent_open,
            COUNT (DISTINCT(CASE WHEN new_to_company = 1 THEN CustomerEmail ELSE NULL END)) AS NewToGWZ,
            COUNT (DISTINCT(CASE WHEN new_to_lob = 1 THEN CustomerEmail ELSE NULL END)) AS NewToLoB
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
    metrics = pd.concat([by_confirmed, by_customer_status, new_customer_acquired, unique_number], axis = 1)
    
    return metrics

# get the metrics for promo campaign
metrics = get_promo_metrics(final, all_short_name)

# #### Details for MolGen and RS

molgen_rs_confirmed_q = """
    SELECT recent_open,
            COUNT(CASE WHEN confirmed = 'non_confirmed' AND LineOfBusinessType = 'Molecular Genetics' THEN CustomerEmail ELSE NULL END) AS MolGen_NonConfirmedOrder,
            SUM(CASE WHEN confirmed = 'non_confirmed' AND LineOfBusinessType = 'Molecular Genetics' THEN BillingAmount ELSE 0 END) AS MolGen_NonConfirmedRevenue,
            COUNT(CASE WHEN confirmed = 'confirmed' AND LineOfBusinessType = 'Molecular Genetics' THEN CustomerEmail ELSE NULL END) AS MolGen_ConfirmedOrder,
            SUM(CASE WHEN confirmed = 'confirmed' AND LineOfBusinessType = 'Molecular Genetics' THEN BillingAmount ELSE 0 END) AS MolGen_ConfirmedRevenue,
            COUNT(CASE WHEN confirmed = 'non_confirmed' AND LineOfBusinessType = 'Regulatory' THEN CustomerEmail ELSE NULL END) AS RS_NonConfirmedOrder,
            SUM(CASE WHEN confirmed = 'non_confirmed' AND LineOfBusinessType = 'Regulatory' THEN BillingAmount ELSE 0 END) AS RS_NonConfirmedRevenue,
            COUNT(CASE WHEN confirmed = 'confirmed' AND LineOfBusinessType = 'Regulatory' THEN CustomerEmail ELSE NULL END) AS RS_ConfirmedOrder,
            SUM(CASE WHEN confirmed = 'confirmed' AND LineOfBusinessType = 'Regulatory' THEN BillingAmount ELSE 0 END) AS RS_ConfirmedRevenue
    FROM final
    GROUP BY recent_open;
    """

molgen_rs_by_confirmed = sqldf(molgen_rs_confirmed_q, locals())
# reindex the table to make it ordered by email sent date
molgen_rs_by_confirmed.set_index('recent_open', inplace=True)
molgen_rs_by_confirmed = molgen_rs_by_confirmed.reindex(all_short_name)


# #### Aggregate the data based on combined emails

################################################
#### 8. Update the email combined name list ####
################################################

final_combined_email = final.copy()
final_combined_email = final_combined_email.drop(['recent_open'], axis = 1).rename(columns={'combined_email':'recent_open'})

all_combined_name = ['Jul NGS WGS Promo', 'Aug NGS Pick Your Promo', 'Jul GS PriorityGENE', 
                    'Jul MolGen/RS Open Quotes Promo', 'Aug MolGen/RS 30% off PCR+Sanger']

# get the metrics for combined emails
metrics_combined_email = get_promo_metrics(final_combined_email, all_combined_name)


# ## Revenue Breakdown

breakdown_q = """
    SELECT recent_open,
            SUM(BillingAmount) AS AllRevenue,
            SUM(CASE WHEN PromotionCode IS NOT NULL THEN BillingAmount ELSE 0 END) AS WithPromoCode
    FROM final
    GROUP BY recent_open;
    """

revenue_breakdown = sqldf(breakdown_q, locals())
# reindex the table to make it ordered by email sent date
revenue_breakdown.set_index('recent_open', inplace=True)
revenue_breakdown = revenue_breakdown.reindex(all_short_name)

# #### Breakdown by promo codes

breakdown_by_code_q = """
    SELECT PromotionCode,
            SUM(BillingAmount) AS AllRevenue
    FROM final
    WHERE PromotionCode = 'NGS19-RNA40' OR PromotionCode = 'NGS19-ATAC' OR PromotionCode = 'NGS19-ATACRNA'
    GROUP BY PromotionCode;
    """

breakdown_by_code = sqldf(breakdown_by_code_q, locals())

# # New Customer List

# new to company
new_to_company_list = final.loc[final['new_to_company'] == 1, 
                                ['CustomerEmail', 'UserName', 'Institution', 'recent_open', 'lob']]
new_to_company_list.insert(loc=0, column='New Customer Type', value='New to GENEWIZ')
# new to lob
new_to_lob_list = final.loc[final['new_to_lob']==1,
                            ['CustomerEmail', 'UserName', 'Institution', 'recent_open', 'lob']]
new_to_lob_list.insert(loc=0, column='New Customer Type', value='New to ' + new_to_lob_list.lob)
new_to_lob_list.loc[:,'lob'] = 'N/A'
new_customer_list_complete = pd.concat([new_to_company_list, new_to_lob_list], 
                                       ignore_index = True, sort = False)
new_customer_list_complete = new_customer_list_complete.rename(columns={'CustomerEmail':'Customer Email', 'UserName':'Customer Name',
                                                                        'recent_open':'Campaign Source', 'lob':'First Order LoB'})
new_customer_list_complete = new_customer_list_complete.drop_duplicates(subset = None, keep = 'first')


# # Export Result

###################################################################
#### 9. Update path and file name before exporting the results ####
###################################################################
# write complete order table to excel file
final.to_excel(r'Z:\August13\Promo\Promo_labeled.xlsx', index=False)

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\August13\Promo\Promo Metrics.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
metrics.to_excel(writer, sheet_name='All Metrics', index=True)
molgen_rs_by_confirmed.to_excel(writer, sheet_name='Molgen & RS', index=True)
revenue_breakdown.to_excel(writer, sheet_name='Revenue Breakdown', index=True)
breakdown_by_code.to_excel(writer, sheet_name='Breakdown By Code', index=True)
metrics_combined_email.to_excel(writer, sheet_name='Metrics by Combined Email', index=True)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

# write new customer list
new_customer_list_complete.to_excel(r'Z:\August13\Promo\Promo New Customer List.xlsx', index=False)

