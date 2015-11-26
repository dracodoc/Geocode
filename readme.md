This is my setup to batch geocode NFIRS data addresses into valid addresses and map to census block.

[Project Background](http://dracodoc.github.io/2015/11/11/Red-Cross-Smoke-Alarm-Project/)
[Environment Setup](http://dracodoc.github.io/2015/11/17/Geocoding/)
[Discussions On Scripts And Work Flow](http://dracodoc.github.io/2015/11/19/Script-workflow/)

To use this work flow:
1. Setup server. Assuming the project folder located in `/home/ubuntu/geocode`.
2. Make address input file, upload to `address_input` folder. Don't use spaces in file name, it will cause error in shell script.
3. Run `python gaddress.py`, then `sh ./batch.sh`. Output file will be put in `address_output` folder.
4. The `log` folder hold the log file of console messages.
