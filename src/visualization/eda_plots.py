import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from typing import Optional, Callable
from itertools import chain

x_dtypes = list | pl.DataFrame | pl.LazyFrame

def barplots_continuous(x: x_dtypes,
                        names: list,
                        legend_title: str = "Column names",
                        norm: Optional[Callable] = None,
                        ):

    x_list = parse_x_input(x, names)

    if norm is not None:
        x_list_norm = list(map(norm, x_list))
    else:
        x_list_norm = x_list

    color = list(chain.from_iterable([i]*len(j) for i, j in zip(names, x_list_norm)))
    x_flat = list(chain.from_iterable(x_list_norm))

    fig = px.histogram(x=x_flat,
                       color=color,
                       marginal="rug",
                       barmode="overlay",
                       opacity=0.7,
                       nbins=20)

    fig.update_layout(legend_title=legend_title)
    return fig


def df_violin_plots(df: pl.DataFrame | pl.LazyFrame, y: str, z: list):

    if not isinstance(df, pl.LazyFrame):
        df = df.lazy()

    names = df_to_list(df.select(z).unique())

    fig = go.Figure()

    for name in sorted(names):
        x_plot = df_to_list(df.filter(pl.col(z) == name).select(z))
        y_plot = df_to_list(df.filter(pl.col(z) == name).select(y))
        fig.add_trace(
            go.Violin(
                x=x_plot,
                y=y_plot,
                name=name,
                box_visible=True,
                meanline_visible=True
            )
        )
    title = f"Violin plot for {y} by {z}"
    fig.update_layout(title=title)
    return fig

def parse_x_input(x: list | pl.DataFrame | pl.LazyFrame,
                  names: list
                  ) -> list:

    if isinstance(x, pl.DataFrame):
        df = x.select(names).drop_nulls()
        return  [df[i].to_list for i in names]
    elif isinstance(x, pl.LazyFrame):
        df = x.select(names).drop_nulls().collect()
        return [df[i].to_list() for i in names]
    elif isinstance(x, list):
        return x
    else:
        return []

def df_to_list(df: pl.DataFrame | pl.LazyFrame) -> list:
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    return list(map(lambda i: i[0], df.rows()))
