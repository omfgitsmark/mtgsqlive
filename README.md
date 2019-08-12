# [**MTGSQLive mySQL version**](https://mtgjson.com/)

# Connect With Us
Discord via [![Discord](https://img.shields.io/discord/224178957103136779.svg)](https://discord.gg/74GUQDE)

Gitter via [![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/mtgjson/mtgjson4)


### Goals
The goals of this project are to extend the MTGJSONv4 protocols and give an option for pre-processed mySQL downloads.
lly edit it to be correct. Once that is accomplished, we are then no longer dependent on them for card data, except for rullings.

# About Us

MTGJSON and MTGSQlive are open sourced database creation and distribution tool for [*Magic: The Gathering*](https://magic.wizards.com/) cards, specifically in [JSON](https://json.org/) and [MySQL](https://www.mysql.com) format.

You can find our documentation with all properties [here](https://mtgjson.com/docs.html).

To provide feedback and/or bug reports, please [open a ticket](https://github.com/mtgjson/mtgsqlite/issues/new/) as it is the best way for us to communicate with the public.

If you would like to join or assist the development of the project, you can [join us on Discord](https://discord.gg/Hgyg7GJ) to discuss things further.

# How To Use

This system was built using *Python 3.7*, so we can only guarantee proper functionality with this version.

```sh
# Install dependencies
$ pip3 install -r requirements.txt 

# usage: mtgsqlive [-h] -s mySQL_server_hostname[:port] -u mySQL_user [-p mySQL_password] -d mySQL_database [-f][-r]
$ python3 -m mtgsqlive -s localhost -u root -p ****** -d mtg

```  

>**NOTE:** The -p parameter is not required, but you will be prompted to enter it during execution if not provided.

>**NOTE:** To provide a port number use socket format *(hostname:port)* for the server parameter, otherwise the default port of 3306 will be used.

