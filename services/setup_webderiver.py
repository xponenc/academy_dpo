from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

###
#  Selenium WebDriver на Windows
###
# https://googlechromelabs.github.io/chrome-for-testing/


# Путь к файлу chromedriver.exe
CHROMEDRIVER_PATH = "C:/WebDriver/chromedriver.exe"


def get_driver() -> webdriver.Chrome:
    """
    Создаёт и настраивает экземпляр Chrome WebDriver.

    :return: Объект WebDriver
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(executable_path=CHROMEDRIVER_PATH)
    return webdriver.Chrome(service=service, options=options)