# ShipTrack project dahsboard for moored ships statistics
[Dashboard](https://st.gabrielfuentes.org) with vessel's visits information generated from AIS data.

Online app that generates an interactive dashboard for a shipping company vessels from January 2020 to September 2022

Directory Tree:

        +---Shiptrack
        |   |   LICENSE
        |   |   Procfile
        |   |   README.md
        |   |   app.py
        |   |   choropleth_trade_map.py
        |   |   controls.py
        |   |   python-app.yml
        |   |   requirements.txt
        |   |   
        |   \---assets
        |   |       favicon.ico
        |   |       s1.css
        |   |       style.css
        |   |       

Dependencies:

      pandas 1.5.1
      dash 2.6.2
      dash_auth 1.3.2
      gunicorn 19.9.0
      geopandas 0.12.1
      requests 2.23.0
      scipy 1.9.3
      geojson 2.5.0
      h3  3.7.4
      pyarrow 10.0.0
      s3fs 0.4.2
      plotly  5.11.0
      orjson  3.8.1

Parameters: 
      
       Parquet files:
       
       AIS fleet positions
       Visits per port per day per vessel
       Date range (weekly) for every port from Jan 2020 to Sep 2022
       Time at berth per vessel visit per port from Jan 2020 to Sep 2022

Returns: 

      Dashboard deployed in st.gabrielfuentes.org via Heroku
        
  
Credits: Gabriel Fuentes Lezcano

Licence: MIT License

Copyright (c) 2022 Gabriel Fuentes

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
© 2022 GitHub, Inc.
