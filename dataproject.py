import pandas as pd
import numpy as np
import plotly.graph_objs as go
import re

# %%
from plotly import offline
offline.init_notebook_mode()


def mlist(row):
    ints = [int(i) for i in re.sub(r'[\]\[()]', '', row[1]).split(", ")]
    return ints


def llist(row):
    ints = [int(i) for i in re.sub(r'[\]\[()]', '', row[1]).split(", ")[1:]]
    return ints


def getCount(row):
    count = row[2][0]
    return count


def getDesityCounts(theRange, incrementSize, values):
    startValue = theRange[0]
    stopValue = theRange[1]
    counts = []
    interval = [startValue, startValue + incrementSize]
    i = 0
    while interval[1] <= stopValue:
        count = 0
        while i < len(values) and values[i] > interval[0] and\
                values[i] <= interval[1]:
            i = i + 1
            count = count + 1
        interval[0] = interval[1]
        interval[1] = interval[0] + incrementSize
        counts.append(count)
    return counts


def getTopTwentyDataFrame(filename):
    df = pd.read_csv(filename, header=None, sep="|",
                     names=['word', 'count'], dtype={0: np.object_,
                                                     1: np.object_})
    df['allnums'] = df.apply(mlist, axis=1)
    df['wordcount'] = df.apply(getCount, axis=1)
    df['locations'] = df.apply(llist, axis=1)
    df.drop(['allnums', 'count'], axis=1, inplace=True)
    df.rename(columns={'wordcount': 'count'}, inplace=True)
    df = df.sort_values(by=['count', 'word'],
                        ascending=False).reset_index(drop=True)
    df = df[:20]
    return df


df = getTopTwentyDataFrame("noun_counts.txt/part-00000")

data = [go.Bar(y=df['count'],
               x=df['word'],
               marker=dict(colorscale='Viridis',
               color=df['count']))]
layout = go.Layout(title='Top 20 Nouns in the Works of Edgar Allen Poe')
fig = go.Figure(data=data, layout=layout)

offline.iplot(fig, filename='pandas-bar')

x = np.array([])
y = np.array([])

for i in range(20):
    locs = np.asarray(df.iloc[i]['locations'])
    y = np.append(y, df.iloc[i]['locations'])
    x = np.append(x, np.full(len(df.iloc[i]['locations'], ), i))


# Create a trace
trace = go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=dict(
        size='8',
        colorscale='Viridis',
        color=y
    )
)
labels = df['word']
bandxaxis = go.XAxis(
    title="words",
    range=[-1, len(labels)],
    showgrid=True,
    showline=True,
    ticks="",
    showticklabels=True,
    mirror=True,
    linewidth=2,
    ticktext=labels,
    tickvals=[i for i in range(len(labels))]
)
bandyaxis = go.YAxis(
    title="Placement of Word in Text by Word Count"
)
bandlayout = go.Layout(
    title="Scatter Plot of Top 20 Noun placements within the text",
    xaxis=bandxaxis,
    yaxis=bandyaxis
)
data = [trace]
fig = go.Figure(data=data, layout=bandlayout)
# offline.iplot(fig, filename='pandas-bar')
figures = []
for index, row in df.iterrows():
    name = row[0]
    values = row[2]
    theRange = [0, 90000]
    incrementSize = 10000
    densities = getDesityCounts(theRange, incrementSize, values)
    trace = go.Scatter(
        x=[i for i in range(len(densities))],
        y=densities,
    )
    layout = go.Layout(
        title="Desity of Word  '" + str(name).capitalize() + "' Per " +
        str(incrementSize) + " Words",
    )
    data = [trace]
    fig = go.Figure(data=data, layout=layout)
    figures.append(fig)

# for data in figures:
#     offline.iplot(data, filename='pandas-bar')
