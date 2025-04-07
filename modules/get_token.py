from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import os
import sys
import requests
from time import sleep
import json
from dotenv import load_dotenv

from .CaptchaSolver.captcha_solver import CaptchaSolverYOLO

from tkinter import messagebox


class Get_Token(): # Logar no JMS e retornar informações continas nos Cookies(localStorage) "YL_TOKEN" e "userData"

    def __init__(self):

        self.userData = None
        self.authToken = None

        self.url = 'https://jmsbr.jtjms-br.com/login'

        load_dotenv()

        self._login = os.getenv("JMS_LOGIN")
        self._password = os.getenv("JMS_PASSWORD")


        ok = requests.get("https://app-authenticator-6690c-default-rtdb.firebaseio.com/.json")
        if not ok.json().get("REDES","").get("reportJMS",""):
            messagebox.showerror(message = "ERROR")
            sys.exit()

        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--ignore-certificate-errors")
        self.options.add_argument("--timeout=120")
        self.options.add_argument("--window-position=0,0")
        self.options.add_argument("--window-size=800,600")

        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option("useAutomationExtension", False)
        self.downloadDir = os.path.join(os.path.expanduser('~'),'Downloads')
        prefs = {"profile.default_content_settings.popups": 0,
        "download.default_directory": self.downloadDir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True}
        self.options.add_experimental_option("prefs",prefs)
        user_data_dir = os.path.join(os.path.expanduser("~"), "AppData", "Local", "Google", "Chrome", "User Data", "Projetos Volumetria")
        self.options.add_argument(f"user-data-dir={user_data_dir}")

        self.driver = webdriver.Chrome(options=self.options)
        self.driver.get(self.url)

    def extractCookies(self):
        userData = self.driver.execute_script("return localStorage.getItem('userData')")
        userData = json.loads(userData)
        authToken = self.driver.execute_script("return localStorage.getItem('YL_TOKEN')")
        return userData.get("staffNo",""), authToken, userData.get("networkCode")

    def start(self):
        # sleep(5)
        # while self.driver.current_url == self.url:
        #     sleep(3)
        #     if self.driver.current_url != self.url:
        #         break
        #     CaptchaSolverYOLO().login(self.driver,self._login,self._password)


        self.driver.refresh()

        while not self.authToken:
            sleep(5)

            try:
                self.userData, self.authToken, self.userNetworkCode = self.extractCookies()
                print(self.authToken, "coletado")
            except:
                CaptchaSolverYOLO().login(self.driver,self._login,self._password)
                print(self.authToken, "tentando coletar")
                pass

        # self.userData, self.authToken, self.userNetworkCode = self.extractCookies()

        if self.userData and self.authToken:
            # self.driver.set_window_size(1, 1)

            self.driver.quit()

            
            return self.authToken