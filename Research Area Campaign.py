#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from pandasql import sqldf


# # Import clean data and historical list

#######################################
#### 1.  Update path of clean data ####
#######################################
# load cleaned data
cleaned = pd.read_excel(r'Z:\Marketing Campaign Tracking Python\August5\CleanedData.xlsx', 
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
# 3. 'combined_name': stands for the combined email name which includes multiple emails
# 4. 'sent_date': stands for the sent dates of all emails
# 5. order types

################################################
#### 3. Update path and name of index table ####
################################################
index_table = pd.read_excel(r'Z:\Clean Code\Research Area Campaign\index_table.xlsx')

# convert sent_date to datetime type
index_table['sent_date'] = pd.to_datetime(index_table['sent_date'])

# if is checked, then 1; if is not, then 0.
index_table.iloc[:, 4:] = (index_table.iloc[:, 4:].notna()).astype(np.int)

# make sure that email_name in index_table is the same as the name of excel file
# remove '/' in email_name in index_table, since name for excel file cannot contain '/'
index_table['long_name'] = index_table.long_name.apply(lambda x: x.replace('/', ''))


# # Import all the open lists and left join with SalesDashboard

# Apply add_open_list function to all the open lists, then we get the salesDashboard table with columns of all the emails.

ready_ra = cleaned2.copy()
ra_all_email_name = list(index_table.long_name)

###################################################################
#### 4. Update path of research area campaign email open lists ####
###################################################################

def add_open_list(email_long_name, ready_data, index_table):
    # import email open lists
    path = r'Z:\Marketing Campaign Tracking Python\July21\Research Area'
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


# loop add_open_list function throught all the email open list.
for i in range(len(ra_all_email_name)):
    ready_ra = add_open_list(ra_all_email_name[i], ready_ra, index_table)
# now we have combined all the email open lists to the salesDashboard


# # Label the email to each order

ra_labeled = ready_ra.copy()
ra_all_email_short_name =list(ra_labeled.columns)[28:]
# drop rows with no email opened
ra_labeled = ra_labeled.dropna(subset=ra_all_email_short_name, how='all')
# Convert NaNs to a very late date 2020-01-31 00:00:00
ra_labeled[ra_all_email_short_name] = ra_labeled.iloc[:,28:].fillna(pd.to_datetime('2020-01-31 00:00:00'))
# Convert all the email sent date format from string to datetime
ra_labeled[ra_all_email_short_name] = ra_labeled.iloc[:,28:].apply(lambda x:pd.to_datetime(x,format = '%Y-%m-%d %H:%M:%S'))
# if the result of the subtraction is positive, it means did not open the email before placing the order
ra_labeled[ra_all_email_short_name] = ra_labeled.iloc[:,28:].apply(lambda x:x-ra_labeled['CreatedDate'])
ra_labeled[ra_all_email_short_name].head()

# ## Subset by order type then label relevant email to each order

def find_recent(x, email_name):
    # find the max negative number in a row
    small_number = pd.Timedelta(days=-99999);
    for i in range(len(email_name)):
        if x[i]<pd.Timedelta(0):
            small_number = max(small_number, x[i])
            
    if small_number == pd.Timedelta(days=-99999):
        return 'none_open'

    #return the index of the item has value data
    for i in email_name:
        if x[i]==small_number:
            return i

# ### Write a function to label the email to a specific order type then loop the function through all the order type
def select_order_type(order_type_name):
    relevant_email = list(index_table.loc[index_table[order_type_name]==1,'short_name'])
    ra_labeled.loc[ra_labeled['OrderType'] == order_type_name,'recent_open'] = ra_labeled[relevant_email].apply(lambda x:find_recent(x, relevant_email),1)
    return ra_labeled

# create a list of all research area
all_order_type = index_table.columns[4:]

# apply the select order function to all order types
for i in range(len(all_order_type)):
    select_order_type(all_order_type[i])


############################################################################################
#### 5. Update path when exporting table with all records in sales dashboard, including ####
####    orders with no open email and order types that are not tracked #####################
############################################################################################
ra_labeled.to_excel(r'Z:\Marketing Campaign Tracking Python\August5\Research Area\RA_labeled_complete.xlsx', index=False)

# Drop customers who have not opened any emails before they placed an order
ra_labeled = ra_labeled[ra_labeled.recent_open != 'none_open']
# Drop recent_open which is NA, those are the order types that we do not want to track
ra_labeled = ra_labeled.dropna(subset = ['recent_open'])


# #### Add a column indicating if is a quote-based order
ra_labeled.loc[ra_labeled['LineOfBusinessType'].isin(['Gene Synthesis', 'Next Gen. Sequencing', 'Molecular Genetics', 'Regulatory']),'quote_based'] = 1
ra_labeled['quote_based'] = ra_labeled.loc[:, 'quote_based'].fillna(0)

# #### Add a column indicating if is non-confirmed
# two conditions:
# if is quote_based, and order status is 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'non_confirmed' 
ra_labeled.loc[(ra_labeled['quote_based']==1)&(ra_labeled['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'non_confirmed'
# if is quote_based, and order status is not 'Cart', 'Discard', 'Ready To Order'
# 'confirmed' should be set as 'confirmed' 
ra_labeled.loc[(ra_labeled['quote_based']==1)&(~ra_labeled['OrderStatus'].isin(['Cart', 'Discard', 'Ready To Order'])),'confirmed'] = 'confirmed'
# for direct order, svalue for column 'confirmed' should be set as 'confirmed'
ra_labeled.loc[ra_labeled['quote_based']==0, 'confirmed'] = 'confirmed'


# ### If new to GWZ

# create an empty dataframe so that we can append dataframe into it later
ra_new_to_gwz = pd.DataFrame()

def new_to_gwz(email_labeled_table, email_short_name, index_table, empty_df):
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
for i in range(len(ra_all_email_short_name)):
    ra_new_to_gwz = new_to_gwz(ra_labeled, ra_all_email_short_name[i], index_table, ra_new_to_gwz)


# ### If new to LoB

# create an empty dataframe so that we can append dataframe into it later
ra_new_to_lob = pd.DataFrame()

def new_to_lob(email_new_to_gwz_added, email_short_name, index_table, empty_df):
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
    tmp_labeled_email = email_new_to_gwz_added.loc[email_new_to_gwz_added['recent_open'] == email_short_name, :]

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
for i in range(len(ra_all_email_short_name)):
    ra_new_to_lob = new_to_lob(ra_new_to_gwz, ra_all_email_short_name[i], index_table, ra_new_to_lob)

# for new to gwz customers, convert the value of new_to_lob to Na
ra_new_to_lob.loc[ra_new_to_lob['new_to_company'] == 1, 'new_to_lob'] = np.nan

ra_final = ra_new_to_lob.copy()

# #### add a column indicating combined email name
def get_combined_name(email_short_name):
    ra_final.loc[ra_final['recent_open']==email_short_name, 'combined_email'] = index_table.loc[index_table['short_name']==email_short_name, 'combined_name'].values[0]
    return ra_final

# loop through all the email name
for i in range(len(ra_all_email_short_name)):
    ra_final = get_combined_name(ra_all_email_short_name[i])


# # Metrics Calculation

def get_ra_metrics(final_data, index_order):
    final = final_data.copy()
    
    # quote-based orders
    ra_quote_q = """
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
    ra_quote = sqldf(ra_quote_q, locals())
    # reindex the table to make it ordered by email sent date
    ra_quote.set_index('recent_open', inplace=True)
    ra_quote = ra_quote.reindex(index_order)
    
    # direct orders
    ra_direct_q = """
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
    ra_direct = sqldf(ra_direct_q, locals())
    # reindex the table to make it ordered by email sent date
    ra_direct.set_index('recent_open', inplace=True)
    ra_direct = ra_direct.reindex(index_order)
    
    # metrics by customer status
    ra_customer_status_q = """
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
    ra_customer_status = sqldf(ra_customer_status_q, locals())
    # reindex the table to make it ordered by email sent date
    ra_customer_status.set_index('recent_open', inplace=True)
    ra_customer_status = ra_customer_status.reindex(index_order)
    
    # number of new customer acquired
    ra_new_customer_acquired_q = """
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
    ra_new_customer_acquired = sqldf(ra_new_customer_acquired_q, locals())
    # reindex the table to make it ordered by email sent date
    ra_new_customer_acquired.set_index('recent_open', inplace=True)
    ra_new_customer_acquired = ra_new_customer_acquired.reindex(index_order)
    
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
    metrics = pd.concat([ra_quote, ra_direct, ra_customer_status, ra_new_customer_acquired, unique_number], axis = 1)
    
    return metrics


# ### Separated
email_separated_metrics = get_ra_metrics(ra_final, ra_all_email_short_name)

# ### Combined
# to calculate the metrics by combined email name, I dropped the recent_open column and changed column name 'combined_email'
# to 'recent_open'
email_combined_final = ra_final.copy()
email_combined_final = email_combined_final.drop(['recent_open'], axis = 1).rename(columns={'combined_email':'recent_open'})
ra_combined_email_name = list(index_table.combined_name.unique())
email_combined_metrics = get_ra_metrics(email_combined_final, ra_combined_email_name)


# # New customer list

# new to company
ra_new_to_company_list = ra_final.loc[ra_final['new_to_company'] == 1, 
                                         ['CustomerEmail', 'UserName', 'Institution', 'combined_email', 'lob']]
ra_new_to_company_list.insert(loc=0, column='New Customer Type', value='New to GENEWIZ')

# new to lob
ra_new_to_lob_list = ra_final.loc[ra_final['new_to_lob']==1,
                                ['CustomerEmail', 'UserName', 'Institution', 'combined_email', 'lob']]
ra_new_to_lob_list.insert(loc=0, column='New Customer Type', value='New to ' + ra_new_to_lob_list.lob)
ra_new_to_lob_list.loc[:,'lob'] = 'N/A'

new_customer_list_complete = pd.concat([ra_new_to_company_list, ra_new_to_lob_list], 
                                       ignore_index = True, sort = False)

new_customer_list_complete = new_customer_list_complete.rename(columns={'CustomerEmail':'Customer Email', 'UserName':'Customer Name',
                                                                        'combined_email':'Campaign Source', 'lob':'First Order LoB'})

new_customer_list_complete = new_customer_list_complete.drop_duplicates(subset = None, keep = 'first')


# # Export Results

###################################################################
#### 6. Update path and file name before exporting the results ####
###################################################################

# write complete order table labeled with combined email name to excel file
ra_final.to_excel(r'Z:\Marketing Campaign Tracking Python\Research Area\RA_labeled.xlsx', index=False)

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(r'Z:\Marketing Campaign Tracking Python\Research Area\Research Area Data.xlsx', engine='xlsxwriter')
# Write each dataframe to a different worksheet.
email_combined_final.to_excel(writer, sheet_name='Email Combined', index=True)
email_separated_metrics.to_excel(writer, sheet_name='Email Seperated', index=True)
# Close the Pandas Excel writer and output the Excel file.
writer.save()

# Write new customer list
new_customer_list_complete.to_excel(r'Z:\Marketing Campaign Tracking Python\Research Area\New Customer List.xlsx', index=False)

