from datetime import datetime
import json
from fasthtml.common import *

from utils import get_predictions


app, rt = fast_app(hdrs=(Script(src="https://cdn.plot.ly/plotly-2.32.0.min.js"),), live=True)


@rt("/")
def get():
    data = get_predictions()
    return Titled(
        "Pranatics - A transaction fee predictive analytics dashboard for the Hedera Network",
        H3(f"{datetime.fromisoformat(data[0]['t_from']).strftime("%A, %H:%M")} - {datetime.fromisoformat(data[0]['t_to']).strftime("%H:%M")}"),
        Div(id="myDiv1"),
        H3(f"{datetime.fromisoformat(data[1]['t_from']).strftime("%A, %H:%M")} - {datetime.fromisoformat(data[1]['t_to']).strftime("%H:%M")}"),
        Div(id="myDiv2"),
        H3(f"{datetime.fromisoformat(data[2]['t_from']).strftime("%A, %H:%M")} - {datetime.fromisoformat(data[2]['t_to']).strftime("%H:%M")}"),
        Div(id="myDiv3"),
        H3(f"{datetime.fromisoformat(data[3]['t_from']).strftime("%A, %H:%M")} - {datetime.fromisoformat(data[3]['t_to']).strftime("%H:%M")}"),
        Div(id="myDiv4"),
        H3(f"{datetime.fromisoformat(data[4]['t_from']).strftime("%A, %H:%M")} - {datetime.fromisoformat(data[4]['t_to']).strftime("%H:%M")}"),
        Div(id="myDiv5"),
        Script(f"var data0 = {json.dumps(data[0])}; Plotly.newPlot('myDiv1', data0);"),
        Script(f"var data1 = {json.dumps(data[1])}; Plotly.newPlot('myDiv2', data1);"),
        Script(f"var data2 = {json.dumps(data[2])}; Plotly.newPlot('myDiv3', data2);"),
        Script(f"var data3 = {json.dumps(data[3])}; Plotly.newPlot('myDiv4', data3);"),
        Script(f"var data4 = {json.dumps(data[4])}; Plotly.newPlot('myDiv5', data4);"),
    )

serve()