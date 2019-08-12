"""
Convert MTGJSON v4 -> mySQL
"""
import argparse
import json
import logging
import pathlib
import mysql.connector
import getpass
from typing import Any, Dict, List, Union

LOGGER = logging.getLogger(__name__)

def main() -> None:
    """
    Main function
    """
    parser = argparse.ArgumentParser()
    #group = parser.add_mutually_exclusive_group()
    #group.add_argument("-f", "--force", action="store_true", help="Force overwrite (Disable warning prompts)")
    #group.add_argument("-r", "--refresh", action="store_true", help="Preserve current database (Update only)")
    parser.add_argument(
        "-i", help="input source (\"AllSets.json\" file or \"AllSetFiles\" directory)", required=True, metavar="fileIn"
    )
    parser.add_argument(
        "-s", help="MySQL Server Hostname", required=True, metavar="hostname"
    )
    parser.add_argument(
        "-u", help="MySQL User", required=True, metavar="user"
    )
    parser.add_argument(
        "-p", help="MySQL Password", metavar="password"
    )
    parser.add_argument(
        "-d", help="MySQL Database", required=True, metavar="database"
    )
    parser.add_argument(
        "-f", help="Force overwrite (Disable warning prompts)", action="store_true"
    )
    parser.add_argument(
        "-r", help="Preserve current database (Update only)", action="store_true"
    )
    args = parser.parse_args()
    
    # Define our I/O paths
    input_file = pathlib.Path(args.i).expanduser()
    if args.p:
        pw = args.p
    else:
        pw = getpass.getpass()
    output = {"host": args.s, "port": "3306", "user": args.u, "passwd": pw, "database": args.d}
    if output["host"].index(":") > -1:
        tmparr = output["host"].split(":")
        output["host"] = tmparr[0] if tmparr[0] else "localhost"
        output["port"] = tmparr[-1]
    
    # Connect and build the MySQL database
    # (NOTE: This should be moved to validate_io_streams eventually, but...problems)
    # option file? https://dev.mysql.com/doc/connector-python/en/connector-python-option-files.html
    try:
        sql_connection = mysql.connector.connect(
            host=output["host"],
            port=output["port"],
            user=output["user"],
            passwd=output["passwd"]
        )
    except mysql.connector.Error as err:
        LOGGER.fatal("Unable to connect to MySQL database. Error: {}".format(err))
        exit()
    LOGGER.info("Successfully connected to MySQL database.")
    cursor = sql_connection.cursor()
    cursor.execute("SET AUTOCOMMIT = 0")
    #cursor.execute("SET sql_notes = 0") # hide warnings
    if not args.r:
        cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=%s",(output["database"],))
        result = cursor.fetchall()
        if len(result) > 0:
            if args.f:
                cursor.execute("DROP DATABASE {}".format(output["database"]))
                sql_connection.commit()
            else:
                answer = input("Database '{}' already exists! Continuing will overwrite it. Continue? (y/n): ".format(output["database"]))
                if (answer.lower() == "y" or answer.lower() == "yes"):
                    cursor.execute("DROP DATABASE {}".format(output["database"]))
                    sql_connection.commit()
                else:
                    # rename?
                    LOGGER.fatal("Database '{}' could not be written. Exiting.".format(output["database"]))
                    exit()
    cursor.execute("CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;".format(output["database"]))
    #cursor.execute("SET sql_notes = 1") # Re-enable warnings
    cursor.execute("Use {};".format(output["database"]))

    if not validate_io_streams(input_file, output):
        exit(1)

    build_sql_schema(sql_connection)
    parse_and_import_cards(input_file, sql_connection, args.r)
    
    sql_connection.close()


def validate_io_streams(input_file: pathlib.Path, output: Dict) -> bool:
    """
    Ensure I/O paths are valid and clean for program
    :param input_file: Input file (JSON)
    :param output_file: Output file (SQLite)
    :return: Good to continue status
    """
    if input_file.is_file():
        # check file extension here
        LOGGER.info("Building using AllSets.json master file.")
    elif input_file.is_dir():
        LOGGER.info("Building using AllSetFiles directory.")  
    else:
        LOGGER.fatal("Invalid input file/directory. ({})".format(input_file))
        return False
    return True


def build_sql_schema(sql_connection: mysql.connector) -> None:
    """
    Create the SQLite DB schema
    :param sql_connection: Connection to the database
    """
    LOGGER.info("Building SQLite Schema")
    cursor = sql_connection.cursor()

    # Build Set table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `sets` ("
        "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "baseSetSize SMALLINT NOT NULL DEFAULT 0,"
        "block VARCHAR(24),"
        "boosterV3 VARCHAR(600),"
        "code VARCHAR(8) NOT NULL UNIQUE,"
        "codeV3 VARCHAR(8),"
        "isForeignOnly TINYINT(1) NOT NULL DEFAULT 0,"
        "isFoilOnly TINYINT(1) NOT NULL DEFAULT 0,"
        "isOnlineOnly TINYINT(1) NOT NULL DEFAULT 0,"
        "isPartialPreview TINYINT(1) NOT NULL DEFAULT 0,"
        "keyruneCode VARCHAR(8),"
        "mcmName VARCHAR(255),"
        "mcmId INTEGER,"
        "meta TEXT," # split into multiple columns {'date': '2019-07-07', 'pricesDate': '2019-07-07', 'version': '4.4.2-rebuild.1'} or seperate table
        "mtgoCode VARCHAR(8),"
        "name VARCHAR(255),"
        "parentCode VARCHAR(8),"
        "releaseDate DATE NOT NULL,"
        "tcgplayerGroupId INTEGER,"
        "totalSetSize SMALLINT NOT NULL DEFAULT 0,"
        "type ENUM('archenemy', 'box', 'core','commander','draft_innovation','duel_deck','expansion','from_the_vault','funny','masters','masterpiece','memorabilia','spellbook','planechase','premium_deck','promo','starter','token','treasure_chest','vanguard') NOT NULL"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Build cards table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `cards` ("
        #"id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "artist VARCHAR(100),"
        "borderColor ENUM('black', 'borderless', 'gold', 'silver', 'white') NOT NULL,"
        "colorIdentity VARCHAR(13),"
        "colorIndicator VARCHAR(13),"
        "colors VARCHAR(13),"
        "convertedManaCost FLOAT,"
        "count SMALLINT,"
        "duelDeck ENUM('a', 'b', 'c') DEFAULT NULL," # remove 'c' after patch
        "faceConvertedManaCost FLOAT,"
        "flavorText VARCHAR(500),"
        "frameEffect ENUM('colorshifted', 'compasslanddfc', 'devoid', 'draft', 'legendary', 'miracle', 'mooneldrazidfc', 'nyxtouched', 'originpwdfc', 'sunmoondfc', 'tombstone') DEFAULT NULL,"
        "frameVersion ENUM('1993', '1997', '2003', '2015', 'future') NOT NULL,"
        "hand VARCHAR(3),"
        "hasFoil TINYINT(1) NOT NULL DEFAULT 0,"
        "hasNonFoil TINYINT(1) NOT NULL DEFAULT 0,"
        "isAlternative TINYINT(1) NOT NULL DEFAULT 0,"
        "isArena TINYINT(1) NOT NULL DEFAULT 0,"
        "isFullArt TINYINT(1) NOT NULL DEFAULT 0,"
        "isMtgo TINYINT(1) NOT NULL DEFAULT 0,"
        "isOnlineOnly TINYINT(1) NOT NULL DEFAULT 0,"
        "isOversized TINYINT(1) NOT NULL DEFAULT 0,"
        "isPaper TINYINT(1) NOT NULL DEFAULT 0,"
        "isPromo TINYINT(1) NOT NULL DEFAULT 0,"
        "isReprint TINYINT(1) NOT NULL DEFAULT 0,"
        "isReserved TINYINT(1) NOT NULL DEFAULT 0,"
        "isStarter TINYINT(1) NOT NULL DEFAULT 0,"
        "isStorySpotlight TINYINT(1) NOT NULL DEFAULT 0,"
        "isTextless TINYINT(1) NOT NULL DEFAULT 0,"
        "isTimeshifted TINYINT(1) NOT NULL DEFAULT 0,"
        "layout ENUM('normal', 'split', 'flip', 'transform', 'meld', 'leveler', 'saga', 'planar', 'scheme', 'vanguard', 'token', 'double_faced_token', 'emblem', 'augment', 'aftermath', 'host') NOT NULL,"
        "life VARCHAR(3),"
        "loyalty VARCHAR(5),"
        "manaCost VARCHAR(50),"
        "mcmId INTEGER DEFAULT 0,"
        "mcmMetaId INTEGER DEFAULT 0,"
        "mcmName VARCHAR(255) DEFAULT NULL,"
        "mtgArenaId INTEGER DEFAULT 0,"
        "mtgoFoilId INTEGER DEFAULT 0,"
        "mtgoId INTEGER DEFAULT 0,"
        "mtgstocksId INTEGER DEFAULT 0,"
        "multiverseId INTEGER DEFAULT 0,"
        "name VARCHAR(255) NOT NULL,"
        "names VARCHAR(255),"
        "number VARCHAR(10),"
        "originalText VARCHAR(1000),"
        "originalType VARCHAR(50),"
        "printings VARCHAR(1000),"
        "power VARCHAR(5),"
        "purchaseUrls VARCHAR(255),"
        "rarity ENUM('basic', 'common', 'uncommon', 'rare', 'mythic') NOT NULL,"
        "scryfallId CHAR(36),"
        "scryfallOracleId CHAR(36),"
        "scryfallIllustrationId CHAR(36),"
        "setCode VARCHAR(8),"
        "INDEX (setCode),"
        "FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,"
        "side ENUM('a', 'b', 'c') DEFAULT NULL,"
        "subtypes VARCHAR(50),"
        "supertypes VARCHAR(20),"
        "tcgplayerProductId INTEGER,"
        "tcgplayerPurchaseUrl VARCHAR(42),"
        "text VARCHAR(1000),"
        "toughness VARCHAR(5),"
        "type VARCHAR(50) NOT NULL,"
        "types VARCHAR(50),"
        "uuid CHAR(36) PRIMARY KEY NOT NULL,"
        "uuidV421 CHAR(36),"
        "variations VARCHAR(3000),"
        "watermark VARCHAR(30)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Build tokens table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `tokens` ("
        #"id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "artist VARCHAR(100),"
        "borderColor ENUM('black', 'borderless', 'gold', 'silver', 'white') NOT NULL,"
        "colorIdentity VARCHAR(13),"
        "colorIndicator VARCHAR(13),"
        "colors VARCHAR(13),"
        "duelDeck ENUM('a', 'b', 'c') DEFAULT NULL," # remove 'c' after patch
        "isOnlineOnly TINYINT(1) NOT NULL DEFAULT 0,"
        "layout ENUM('normal', 'split', 'flip', 'transform', 'meld', 'leveler', 'saga', 'planar', 'scheme', 'vanguard', 'token', 'double_faced_token', 'emblem', 'augment', 'aftermath', 'host') NOT NULL,"
        "loyalty VARCHAR(5),"
        "name VARCHAR(255) NOT NULL,"
        "names VARCHAR(255),"
        "number VARCHAR(10),"
        "power VARCHAR(5),"
        "reverseRelated VARCHAR(1000),"
        "scryfallId CHAR(36),"
        "scryfallOracleId CHAR(36),"
        "scryfallIllustrationId CHAR(36),"
        "setCode VARCHAR(8) NOT NULL,"
        "INDEX (setCode),"
        "FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE,"
        "side ENUM('a', 'b', 'c') DEFAULT NULL,"
        "text VARCHAR(1000),"
        "toughness VARCHAR(5),"
        "type VARCHAR(50),"
        "uuid CHAR(36) PRIMARY KEY NOT NULL,"
        "uuidV421 CHAR(36),"
        "watermark VARCHAR(30)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Translations for set names
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `set_translations` ("
        "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "language ENUM('Chinese Simplified', 'Chinese Traditional', 'French', 'German', 'Italian', 'Japanese', 'Korean', 'Portuguese (Brazil)', 'Russian', 'Spanish') NOT NULL,"
        "translation VARCHAR(100),"
        "setCode VARCHAR(8) NOT NULL,"
        "INDEX (setCode),"
        "FOREIGN KEY (setCode) REFERENCES sets(code) ON UPDATE CASCADE ON DELETE CASCADE"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Build foreignData table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `foreignData` ("
        "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "uuid CHAR(36) NOT NULL,"
        "INDEX (uuid),"
        "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE,"
        "flavorText TEXT,"
        "language ENUM('English', 'Spanish', 'French', 'German', 'Italian', 'Portuguese (Brazil)', 'Japanese', 'Korean', 'Russian', 'Chinese Simplified', 'Chinese Traditional', 'Hebrew', 'Latin', 'Ancient Greek', 'Arabic', 'Sanskrit', 'Phyrexian') NOT NULL,"
        "multiverseId INTEGER,"
        "name VARCHAR(255) NOT NULL,"
        "text VARCHAR(1200),"
        "type VARCHAR(255)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Build legalities table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `legalities` ("
        "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "uuid CHAR(36) NOT NULL,"
        "INDEX (uuid),"
        "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE,"
        "format ENUM('standard', 'modern', 'legacy', 'vintage', 'commander', 'duel', 'frontier', 'future', 'oldschool', 'penny', 'pauper', 'brawl') NOT NULL,"
        "status ENUM('Legal', 'Restricted', 'Banned', 'Future') NOT NULL"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Build ruling table
    cursor.execute("CREATE TABLE IF NOT EXISTS `rulings` ("
        "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "uuid CHAR(36) NOT NULL," 
        "INDEX (uuid),"
        "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE,"
        "date DATE," 
        "text VARCHAR(2000)" 
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Build prices table
    cursor.execute("CREATE TABLE IF NOT EXISTS `prices` ("
        "id INTEGER PRIMARY KEY AUTO_INCREMENT,"
        "uuid CHAR(36) NOT NULL,"
        "INDEX (uuid),"
        "FOREIGN KEY (uuid) REFERENCES cards(uuid) ON UPDATE CASCADE ON DELETE CASCADE,"
        "type ENUM('paper', 'paperFoil', 'online') NOT NULL,"
        "price REAL,"
        "date DATE"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    )

    # Execute the commands
    sql_connection.commit()


def parse_and_import_cards(
    input_file: pathlib.Path, sql_connection: mysql.connector, refresh: bool
) -> None:
    """
    Parse the JSON cards and input them into the database
    :param input_file: AllSets.json file
    :param sql_connection: Database connection
    """
    if input_file.is_file():
        LOGGER.info("Loading JSON into memory")
        json_data = json.load(input_file.open("r", encoding="utf8"))

        LOGGER.info("Building sets")
        for set_code, set_data in json_data.items():
            # Handle set insertion
            LOGGER.info("Inserting set row for {}".format(set_code))
            set_insert_values = handle_set_row_insertion(set_data)
            sql_dict_insert(set_insert_values, "sets", sql_connection, refresh)

            for card in set_data.get("cards"):
                LOGGER.debug("Inserting card row for {}".format(card.get("name")))
                card_attr: Dict[str, Any] = handle_card_row_insertion(card, set_code)
                sql_insert_all_card_fields(card_attr, sql_connection, refresh)

            for token in set_data.get("tokens"):
                LOGGER.debug("Inserting token row for {}".format(token.get("name")))
                token_attr = handle_token_row_insertion(token, set_code)
                sql_dict_insert(token_attr, "tokens", sql_connection, refresh)

            for language, translation in set_data["translations"].items():
                LOGGER.debug("Inserting set_translation row for {}".format(language))
                set_translation_attr = handle_set_translation_row_insertion(language, translation, set_code)
                sql_dict_insert(set_translation_attr, "set_translations", sql_connection, refresh)
    elif input_file.is_dir():
        for setFile in input_file.glob("*.json"):
            LOGGER.info("Loading {} into memory...".format(setFile.name))
            set_data = json.load(setFile.open("r", encoding="utf8"))
            set_code = setFile.stem
            LOGGER.info("Building set: {}".format(set_code))
            set_insert_values = handle_set_row_insertion(set_data)
            sql_dict_insert(set_insert_values, "sets", sql_connection, refresh)
            
            for card in set_data.get("cards"):
                LOGGER.debug("Inserting card row for {}".format(card.get("name")))
                card_attr: Dict[str, Any] = handle_card_row_insertion(card, set_code)
                sql_insert_all_card_fields(card_attr, sql_connection, refresh)

            for token in set_data.get("tokens"):
                LOGGER.debug("Inserting token row for {}".format(token.get("name")))
                token_attr = handle_token_row_insertion(token, set_code)
                sql_dict_insert(token_attr, "tokens", sql_connection, refresh)

            for language, translation in set_data["translations"].items():
                LOGGER.debug("Inserting set_translation row for {}".format(language))
                set_translation_attr = handle_set_translation_row_insertion(language, translation, set_code)
                sql_dict_insert(set_translation_attr, "set_translations", sql_connection, refresh)
    sql_connection.commit()


def sql_insert_all_card_fields(
    card_attributes: Dict[str, Any], sql_connection: mysql.connector, refresh: bool
) -> None:
    """
    Given all of the card's data, insert the data into the
    appropriate SQLite tables.
    :param card_attributes: Tuple of data
    :param sql_connection: DB Connection
    """
    sql_dict_insert(card_attributes["cards"], "cards", sql_connection, refresh)

    for foreign_val in card_attributes["foreignData"]:
        sql_dict_insert(foreign_val, "foreignData", sql_connection, refresh)

    for legal_val in card_attributes["legalities"]:
        sql_dict_insert(legal_val, "legalities", sql_connection, refresh)

    for rule_val in card_attributes["rulings"]:
        sql_dict_insert(rule_val, "rulings", sql_connection, refresh)

    for price_val in card_attributes["prices"]:
        sql_dict_insert(price_val, "prices", sql_connection, refresh)
        

def handle_set_row_insertion(set_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    This method will take the set data and convert it, preparing
    for SQLite insertion
    :param set_data: Data to process
    :return: Dictionary ready for insertion
    """
    set_skip_keys = ["cards", "tokens", "translations"]
    set_insert_values = {}

    for key, value in set_data.items():
        if key in set_skip_keys:
            continue

        if key == "boosterV3":
            set_insert_values[key] = modify_for_sql_insert(str(value))
            continue

        set_insert_values[key] = modify_for_sql_insert(value)

    return set_insert_values


def handle_foreign_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts ready for insertion
    """

    foreign_entries = []
    for entry in card_data["foreignData"]:
        foreign_entries.append(
            {
                "uuid": card_uuid,
                "flavorText": entry.get("flavorText", ""),
                "language": entry.get("language", ""),
                "multiverseId": entry.get("multiverseId", 0),
                "name": entry.get("name", ""),
                "text": entry.get("text", ""),
                "type": entry.get("type", ""),
            }
        )

    return foreign_entries


def handle_legal_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    legalities = []
    for card_format, format_status in card_data["legalities"].items():
        legalities.append(
            {"uuid": card_uuid, "format": card_format, "status": format_status}
        )

    return legalities

def handle_ruling_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    rulings = []
    for rule in card_data["rulings"]:
        rulings.append(
            {
                "uuid": card_uuid,
                "date": rule.get("date", ""),
                "text": rule.get("text", ""),
            }
        )
    return rulings

def handle_price_rows(
    card_data: Dict[str, Any], card_uuid: str
) -> List[Dict[str, Any]]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param card_uuid: UUID to be used as a key
    :return: List of dicts, ready for insertion
    """
    prices = []
    for type in card_data["prices"]:
        for date, price in card_data["prices"][type].items():
            prices.append(
                {"uuid": card_uuid, "type": type, "price": price, "date": date}
            )

    return prices

def handle_set_translation_row_insertion(
    language: str,
    translation: str,
    set_name: str
) -> Dict[str, Any]:
    """
    This method will take the set translation data and convert it, preparing
    for SQLite insertion
    :param language: The language of the set translation
    :param translation: The set name translated in to the given language
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    set_translation_insert_values: Dict[str, Any] = {
        "language": language,
        "translation": translation,
        "setCode": set_name
    }

    return set_translation_insert_values


def handle_token_row_insertion(
    token_data: Dict[str, Any], set_name: str
) -> Dict[str, Any]:
    """
    This method will take the token data and convert it, preparing
    for SQLite insertion
    :param token_data: Data to process
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    token_insert_values: Dict[str, Any] = {"setCode": set_name}
    for key, value in token_data.items():
        token_insert_values[key] = modify_for_sql_insert(value)

    return token_insert_values


def handle_card_row_insertion(
    card_data: Dict[str, Any], set_name: str
) -> Dict[str, Any]:
    """
    This method will take the card data and convert it, preparing
    for SQLite insertion
    :param card_data: Data to process
    :param set_name: Set name, as it's a card element
    :return: Dictionary ready for insertion
    """
    # ORDERING MATTERS HERE
    card_skip_keys = ["foreignData", "legalities", "rulings", "prices"]

    card_insert_values: Dict[str, Any] = {"setCode": set_name}
    for key, value in card_data.items():
        if key in card_skip_keys:
            continue
        card_insert_values[key] = modify_for_sql_insert(value)

    foreign_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[0] in card_data.keys():
        foreign_insert_values = handle_foreign_rows(card_data, card_data["uuid"])

    legal_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[1] in card_data.keys():
        legal_insert_values = handle_legal_rows(card_data, card_data["uuid"])

    ruling_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[2] in card_data.keys():
        ruling_insert_values = handle_ruling_rows(card_data, card_data["uuid"])

    price_insert_values: List[Dict[str, Any]] = []
    if card_skip_keys[3] in card_data.keys():
        price_insert_values = handle_price_rows(card_data, card_data["uuid"])

    return {
        "cards": card_insert_values,
        "foreignData": foreign_insert_values,
        "legalities": legal_insert_values,
        "rulings": ruling_insert_values,
        "prices": price_insert_values,
    }


def modify_for_sql_insert(data: Any) -> Union[str, int, float]:
    """
    Arrays and booleans can't be inserted, so we need to stringify
    :param data: Data to modify
    :return: string value
    """
    if isinstance(data, (str, int, float)):
        return data

    if isinstance(data, list) and data and isinstance(data[0], str):
        return ", ".join(data)

    if isinstance(data, bool):
        return int(data)

    if isinstance(data, dict):
        return str(data)

    return ""


def sql_dict_insert(
    data: Dict[str, Any], table: str, sql_connection: mysql.connector, refresh: bool
) -> None:
    """
    Insert a dictionary into a sqlite table
    :param data: Dict to insert
    :param table: Table to insert to
    :param sql_connection: SQL connection
    """
    cursor = sql_connection.cursor()
    if refresh:
        if table in ["cards", "sets", "tokens"]:
            query = "INSERT INTO " + table + " (" + ", ".join(data.keys()) + ") VALUES (" + ", ".join(["%s"] * len(data)) + ") ON DUPLICATE KEY UPDATE " + "=%s, ".join(data.keys()) + "=%s"
            cursor.execute(query, list(data.values()) + list(data.values()))
        else:
            cursor.execute("SELECT id FROM " + table + " WHERE " + "=%s AND ".join(data.keys()) + "=%s", list(data.values()))
            result = cursor.fetchall()
            if len(result) == 0:
                query = "INSERT INTO " + table + " (" + ", ".join(data.keys()) + ") VALUES (" + ", ".join(["%s"] * len(data)) + ")"
                cursor.execute(query, list(data.values()))
    else:
        query = "INSERT INTO " + table + " (" + ", ".join(data.keys()) + ") VALUES (" + ", ".join(["%s"] * len(data)) + ")"
        #print(data.values())
        cursor.execute(query, list(data.values()))
