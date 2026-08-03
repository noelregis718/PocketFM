import pandas as pd
df = pd.read_excel('vanshika_part1.xlsx')
zeros = df[(df['Num_Primary_Books_in_Series'] > 0) & ((df['Total_Page_Count_of_Primary_Books'] == 0) | (df['Total_Page_Count_of_Primary_Books'] == 0.0))]
print('Rows with 0 page count:')
for idx, row in zeros.iterrows():
    print(f'Row {idx}: {row["Book Title"]} | {row["GoodReads_Series_URL"]}')
