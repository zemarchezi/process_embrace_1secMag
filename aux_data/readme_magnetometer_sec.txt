For more information about the data in this directory, please see

http://www2.inpe.br/climaespacial/

This directory contains the raw data files that are used to create the maps of the Magnetometer Application

The directory follows the structure:

	- magnetometer (main folder)
            - year
                - station
                    - data

The data file name is STNDOYHH.zip, where:
STN: station label (3 letters)
DOY: day of the year
HH: hour (2 difgits)


inside the zip:


The data file name is STNDOYHH.YYs, where:
STN: station label (3 letters)
DOY: day of the year
HH: hour (2 difgits)
YY: year (2 digits)
s: letter s (means the data sample is in seconds)


Data files start with a 5-line header similar to:
CACHOEIRA PAULISTA EMBRACE-05 <005> 1 Sec. Raw data

HH MM SS   H(Ch2)    D(Ch4)    Z(Ch6)    T1(Ch7)   T2(Ch8)


Then follows the data in 10 columns:
column 1: mour of the day UTC
column 2: minute UTC
column 3: second UTC
column 4: chanel raw horizontal component
column 5: chanel raw declination component
column 6: chanel raw vertical component
column 7: chanel raw temperature 1
column 8: chanel raw temperature 2

station coordinates:
longitude  latitude station
-56.10403  -9.87033 alf (deactivated)
-48.07352  -5.65130 ara
-56.06945 -15.55472 cba
-71.99485 -36.64122 chi
-45.01443 -22.70207 cxp
-38.42467  -3.87998 eus
-51.71840 -17.93185 jat
-54.11451 -25.29967 med
-59.97465  -3.10803 man (deactivated)
-48.35889 -10.17889 pal
-63.94000   -8.8350 pve
-67.75145 -53.78572 rga
-45.96353 -23.20862 sjc
-44.20972  -2.59417 slz
-54.53920  -2.68630 stm
-53.82273 -29.44357 sms
-65.31555 -26.79612 tcm
-43.65223 -22.40197 vss
