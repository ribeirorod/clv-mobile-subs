import pandas as pd


# Close output file before running!
# create an blank excel file to append the results 

output_file = 'data/output.xlsx'
with pd.ExcelWriter(output_file, mode='w') as writer:
    pd.DataFrame([]).to_excel(writer)


levels = ['joined_week','country', 'network_operator', 'advertiser', 'periodicity']


def pMaxExport (input_joined, output, levels):
    aggregation= {'amount': 'sum', 'volume':'sum','Payout':'mean'}
    for i in range (len(levels), 1, -1):

        columns = levels[:i]
        group=input_joined.groupby(columns).agg({'joins':'sum'}).reset_index().fillna(0)

        output = output.groupby(columns).agg(aggregation).reset_index(). \
            merge(group, on=columns, how='left')
        
        # user view - pMax related fields 
        output['bltv'] = (output['amount'] / output['volume']).fillna(0.0)
        output['maxcpa'] = ((output['amount'] * output['Payout']) / output['volume']).fillna(0.0) 
        output['jltv'] = (output['amount'] / output['joins']).fillna(0.0) 
        output['maxcpo'] = ((output['amount'] * output['Payout']) / output['joins']).fillna(0.0) 
        

        with pd.ExcelWriter( output_file, mode='a') as writer:
            output.to_excel(writer, sheet_name='level_%s' %columns[-1], index= False)

pMaxExport(input_joins, consolidate, levels)