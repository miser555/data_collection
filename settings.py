import os

MSG_FOLDER = os.path.join(".", "msg")
XML_FOLDER = os.path.join(".", "xml")

if not os.path.exists(MSG_FOLDER):
    os.makedirs(MSG_FOLDER)

if not os.path.exists(XML_FOLDER):
    os.makedirs(XML_FOLDER)


OH_API_ENDPOINT = "https://www.openhub.net/"
OH_API_KEY = "c41860f05d55b59896afacf341dfc5853c8fa6344cae24d064c611e060452a37"
#OH_API_KEY = "45f9e6ab501adaade3fc276d98cfd2f528af71bdb90a1524af2b1c3e9d57e18e"
