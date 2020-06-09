# data_engineering_project
Data Engineering Project Stock Data Analysis


### To start you should open the GUI to run the data. Set each script to True in http://localhost:2001 then click run_all button
```
git clone https://github.com/Chrisebell24/data_engineering_project.git
pip install dash --user
pip install sqlalchemy --user
cd data_engineering_project
python _dash_routine_gui.py
```
### exch_nyse.py 
uses FTP from the New York Stock Exchange to get a list of symbols. Updates active = 1 or active = 0

### prices & dividends
Store prices and dividends into tables. prices_register and dividends_register SQL tables will be used for updating on stock splits or other actions that would cause the previous close price to be something different. Uses yahoo finance as the source.

### cboe.py
CBOE uses selenium webbrowser to get a list of URL and store in cboe_urls SQL table so you don't pull links that have historically already been pulled. After 7 days of new new data, it will stop pulling from a URL.
