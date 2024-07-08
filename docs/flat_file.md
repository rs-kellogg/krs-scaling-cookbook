## Scaling Example: Flat files

We'll work with a [dataset](https://www.kaggle.com/datasets/sunnykakar/spotify-charts-all-audio-data) of Spotify Charts available on Kaggle. Here's the description according to the authors.


> This is a complete dataset of all the "Top 200" and "Viral 50" charts published globally by Spotify. Spotify publishes a new chart every 2-3 days. This is its entire collection since January 1, 2019. This dataset is a continuation of the Kaggle Dataset: Spotify Charts but contains 29 rows for each row that was populated using the Spotify API.

To download the data, you only need to create a free Kaggle account. The downloaded data is a zipped archive which contains one CSV file called "merged_data.csv". Before loading in any data, we can use the disk utilities via Python's os module to see the size of the file.


```python
import os
import sys

def bytes_to_human_readable(size):
    units = ['bytes', 'KB', 'MB', 'GB', 'TB']
    for i in range(len(units)):
        if size < 1024:
            break
        size /= 1024
    return size, units[i]

def show_file_size(path):
    ## get file size in bytes
    file_size = os.path.getsize(path)

    ## convert to human readable format
    file_size, unit = bytes_to_human_readable(file_size)
    print(f'File size: {file_size:.2f} {unit}')
    
def show_dataframe_memory_usage(df,deep=False):
    ## get memory usage in bytes
    mem_usage = df.memory_usage(deep=deep).sum()

    ## convert to human readable format
    mem_usage, unit = bytes_to_human_readable(mem_usage)
    print(f'DataFrame memory usage: {mem_usage:.2f} {unit}')
    
show_file_size('merged_data.csv')
```

    File size: 25.24 GB


You can also use the "ls -lh" command to list the files and their size. The -l tag puts the files into a stacked list and -h gives the size in "human" units (KB, MB, GB, etc., instead of bytes.)


```python
ls -lh merged_data.csv
```

    -rw-r--r-- 1 jat4714 jat4714 26G Jul  6 14:51 merged_data.csv


#### Note on compressed data

While this tutorial is mostly about conserving memory, now might also be a good time to talk about conserving hard disk space as well. You can compress CSV files and still load in the dataset into Python without needing to decompress them. I used gzip to compress the CSV file in this example and you can see it takes up way less space.


```python
show_file_size('merged_data.csv.gz')
```

    File size: 2.83 GB


However, while we've managed to compress the data in its archived state, we still need to worry about the size of the uncompressed data. Unfortunately, the compression ratio is can vary a lot depending on the type of data and method of compression, so it is not trivial to calculate the size of the decompressed data before decompression.

If you are working on a laptop, you most likely have 2-8 GB of RAM. Therefore, we cannot load the entire dataset into memory. So, what do we do?

### Data exploration

We can read a preview the compressed CSV file directly into Pandas using a couple of additional keyword parameters.

- nrows: number of rows (beyond the header) that you want to load
- compression: the name of the algorithm used to compress the file. Pandas is usually smart enough to figure this out based on the file extension, but it's safer to provide this directly. A gzipped file will usually have the extension '.gz', so we'll put 'gzip' here.



```python
import pandas as pd
pd.set_option('display.max_columns', None)

df = pd.read_csv('merged_data.csv.gz', 
                 compression='gzip',
                 nrows=10_000)
```


```python
## df.info() will also show memory usage
show_dataframe_memory_usage(df,deep=False)
```

    DataFrame memory usage: 2.15 MB



```python
## df.info(memory_usage='deep') also displays this info
show_dataframe_memory_usage(df,deep=True)
```

    DataFrame memory usage: 13.40 MB


The data itself takes up 2.1 MB. However, the entire object takes up 13.4 MB. This is because of all of the indexing and metadata stored in the object itself. Therefore, it's a good rule of thumb multiply the file size by 10 when budgeting memory usage. This effect is true of both R and Stata.

### Column selection and data type assignment

You can save space if you only select the columns that are relevant for your work. It can also be taxing on the memory usage for the function to automatically assign data types to each column. You can specify the data types ahead of time with a dictionary and using Pandas data types.

Let's say we want to track the performance of a specific song in a specific region on the Spotify Charts. We only need to track the rank and date after filtering on region, artist, and song.


```python
dtypes = {
    'title': pd.StringDtype(),
    'rank': pd.Int32Dtype(),
    'date': pd.StringDtype(),
    'artist': pd.StringDtype(),
    'region': pd.StringDtype(),
    'chart': pd.StringDtype(),
}

df = pd.read_csv('merged_data.csv.gz', 
                 compression='gzip',
                 nrows=10_000,
                 usecols=dtypes.keys(),
                 dtype=dtypes)

```

### Breaking the analysis up into chunks

Now we need to process the remaining dataset. We can use the chunksize parameter to only pull out a few thousand rows at a time for processing before we continue. This keyword parameter causes the function to output a generator object, which is an iterable object but only one value is kept in memory at a time.

Below is an example of really simple generator. This function is the equivalent of the range function, but only pulls out one value at a time.

Here's a function that will loop through each chunk of the data, filter on the song/artist and region, and append data to an existing frame. We also want to keep the memory footprint of the filtered data, so we'll put a cap on the total number of rows we can keep in memory and periodically dump the results to hard disk.


```python
def filter_data(title=None, artist=None, region=None):
    dtypes = {
        'title': pd.StringDtype(),
        'rank': pd.Int32Dtype(),
        'date': pd.StringDtype(),
        'artist': pd.StringDtype(),
        'region': pd.StringDtype(),
        'chart': pd.StringDtype()
    }

    filtered_data = pd.DataFrame()
    i = 0
    
    ## read in only 10k rows at a time and only the columns we care about
    for df in pd.read_csv('merged_data.csv.gz', 
                    compression='gzip',
                    chunksize=10_000,
                    usecols=dtypes.keys(),
                    dtype=dtypes):
        
        # select only the top200 chart
        df = df[df['chart'].eq('top200')]
            
        ## filter by title, artist, region
        if title is not None:
            df = df[df['title'].str.contains(title, case=False)]
        if artist is not None:
            df = df[df['artist'].str.contains(artist, case=False)]
        if region is not None:
            df = df[df['region'].str.contains(region, case=False)]
        
        ## "Smart" way (i.e. memory-saving) to append data!
        ## append to filtered_data if the total number of rows is less than 10_000
        ## otherwise dump data and create a new frame
        if len(filtered_data) + len(df) <= 10_000:
            filtered_data = pd.concat([filtered_data, df])
        else:
            filtered_data.to_csv(f'filtered_data_{i}.csv', index=False)
            i += 1
            filtered_data = df
            
        ## save the last chunk
        filtered_data.to_csv(f'filtered_data_{i}.csv', index=False)

```

Let's pull out all Taylor Swift songs on the top 200 chart in the US.


```python
## let's pull out all Taylor Swift songs in the United States
filter_data(artist='taylor swift', 
            region='united states')
```


```python
import glob
len(glob.glob('filtered_data_*.csv'))
```




    1



The output generated only one file (<10k records), so we are safe to read all the filtered data into memory.


```python
df = pd.concat([pd.read_csv(f,dtype=dtypes) for f in glob.glob('filtered_data*.csv')])
```


```python
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>rank</th>
      <th>date</th>
      <th>artist</th>
      <th>region</th>
      <th>chart</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>I Don’t Wanna Live Forever (Fifty Shades Darke...</td>
      <td>15</td>
      <td>2017-01-01</td>
      <td>ZAYN, Taylor Swift</td>
      <td>United States</td>
      <td>top200</td>
    </tr>
    <tr>
      <th>1</th>
      <td>End Game</td>
      <td>193</td>
      <td>2018-03-01</td>
      <td>Taylor Swift, Ed Sheeran, Future</td>
      <td>United States</td>
      <td>top200</td>
    </tr>
    <tr>
      <th>2</th>
      <td>I Don’t Wanna Live Forever (Fifty Shades Darke...</td>
      <td>9</td>
      <td>2017-01-02</td>
      <td>ZAYN, Taylor Swift</td>
      <td>United States</td>
      <td>top200</td>
    </tr>
    <tr>
      <th>3</th>
      <td>I Don’t Wanna Live Forever (Fifty Shades Darke...</td>
      <td>3</td>
      <td>2017-02-01</td>
      <td>ZAYN, Taylor Swift</td>
      <td>United States</td>
      <td>top200</td>
    </tr>
    <tr>
      <th>4</th>
      <td>I Don’t Wanna Live Forever (Fifty Shades Darke...</td>
      <td>6</td>
      <td>2017-01-03</td>
      <td>ZAYN, Taylor Swift</td>
      <td>United States</td>
      <td>top200</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
