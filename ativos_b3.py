import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

project_directory = os.path.dirname(os.path.abspath(__file__)) 

chrome_options = Options()
# chrome_options.add_argument("headless") 
# Options.headless = True

chrome_options.add_experimental_option('prefs', {
    "download.default_directory": os.path.join(project_directory, "ativos"),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "headless": True
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

url = 'https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/dados-publicos-de-produtos-listados-e-de-balcao/'
driver.get(url)

wait = WebDriverWait(driver, 2)

try:
    iframe = wait.until(EC.presence_of_element_located((By.ID, "bvmf_iframe")))
    driver.switch_to.frame(iframe)

    time.sleep(2)

    driver.execute_script("window.scrollTo(0, 320);")

    download_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "/html/body/div/div/div/div/div[2]/div[1]/div/div[1]/div[2]/div/div/div[1]/div[2]/p[2]/a[1]")))

    download_button.click()

    time.sleep(3)
finally:
    driver.quit()
