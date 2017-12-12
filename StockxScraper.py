from StockXDict import sneakers, columns, streetwear
import os
import pandas as pd
import requests
import concurrent.futures

def scrape(productID):
    url = 'https://stockx.com/api/products/' + productID + '/activity?state=480' 
    page = requests.get(url)
    data = page.text
    df = pd.read_json(data)
    try: 
       df = df[columns]
    except:
        pass
    return(df)

def construct_df(df1, OGdf):
	df = pd.concat([df1, OGdf], join='inner', ignore_index=True, axis=0)
	df.drop_duplicates(keep='first', inplace=True)
	df['createdAt'] = df['createdAt'].replace('T', ' ')
	df['createdAt'] = pd.to_datetime(df['createdAt'], format='%Y%m%d %H:%M') #infer_datetime_format=True
	df.sort_values(by='createdAt', ascending=False)
	df.index = range(0, len(df))
	return df

    
def update(sneakerlist):
    for sneaker in sneakerlist:
        df = scrape(sneaker[1])
        path = f'sneaker_sales/{sneaker[0]}.csv'
        
        if os.path.isfile(path):
            OGdf = pd.read_csv(path)
            
            if not OGdf.empty:
              df = construct_df(df, OGdf)

        df.to_csv(path)

def swupdate(streetwearlist):
	for piece in streetwearlist:
		df = scrape(piece[1])
		path = f'streetwear_sales/{piece[0]}.csv'
        
		if os.path.isfile(path):
			OGdf = pd.read_csv(path)
		    
			if not OGdf.empty:
				df = construct_df(df, OGdf)
		        
		df.to_csv(path)


if __name__ == '__main__':
	with concurrent.futures.ProcessPoolExecutor() as executor:
		executor.submit(update, list(sneakers.items()))
		executor.submit(swupdate, list(streetwear.items()))