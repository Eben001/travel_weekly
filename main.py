import cloudscraper
from bs4 import BeautifulSoup
import requests
import pandas as pd
from requests.exceptions import SSLError, ConnectionError
from telegram import Bot
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

telegram_token = '6596910288:AAEhNR0tb_2e5bUQRWbsHyKn55d6PHWnChw'
telegram_chat_id = '7079224492'
bot = Bot(token=telegram_token)

scraper = cloudscraper.create_scraper()
global_data_list = []
if asyncio.get_event_loop().is_running():
    import nest_asyncio
    nest_asyncio.apply()

# response = scraper.get("https://www.travelweekly.com/Hotels/Berlin/Dorint-Kurfurstendamm-Berlin-p56404921")
# response = scraper.get("https://www.travelweekly.com/Hotels/Berlin/DORMERO-Hotel-Berlin-Kudamm-p9736297")
# response = scraper.get("https://www.travelweekly.com/Hotels/Berlin/Hotel-Am-Steinplatz-Autograph-Collection-p7122601")

async def send_file_to_telegram(filename):
    with open(filename, 'rb') as file:
        await bot.send_document(chat_id=telegram_chat_id, document=file)

async def send_telegram_message(text):
    await bot.send_message(chat_id=telegram_chat_id, text=text)




def get_hotel_details_with_retry(link):
    retry_attempts = 5
    for attempt in range(retry_attempts):
      try:
          response = scraper.get(link)

          if response.status_code == 200:
              return response
          elif response.status_code == 403 or str(response.status_code).startswith('5'):
              print(f"Error {response.status_code} \nRetrying... Attempt {attempt+1}/{retry_attempts}")
              time.sleep(3)
          else:
              return response
      except requests.exceptions.RequestException as e:
          print("Error:", e)
      except SSLError as e:
          print("SSLError:", e)
      except ConnectionError as e:
            print("ConnectionError:", e)

def get_hotels_details(link):
    response = scraper.get(link)

    soup = BeautifulSoup(response.text, 'lxml')

    try:
      name = soup.find('h1',class_='title-xxxl').text.strip()
    except:
      name = ''

    try:
      address = soup.find('div', class_='address').text.strip()
    except:
      address = ''


    try:
      contact_div = soup.find('div', class_='contact')
      phone = contact_div.find('b', string='Phone:').next_sibling.strip()
    except:
      phone = ''
    try:
      contact_div = soup.find('div', class_='contact')
      fax = contact_div.find('b', string='Fax:').next_sibling.strip()
    except:
      fax = ''


    try:
      hotel_email = soup.find('a', {'title': 'Hotel E-mail'}).get('href').split(':')[1]
    except:
      hotel_email = ''

    try:
      hotel_website = soup.find('a', {'title': 'Hotel Website'}).get('href')
    except:
      hotel_website = ''

    try:
      classification = soup.find('td', class_='class').text.strip()
    except:
      classification = ''

    try:
      commission = soup.find('td', class_='comm').text.strip()
    except:
      commission = ''

    try:
      rooms = soup.find('td', class_='rooms').text.strip()
    except:
      rooms = ''

    try:
      rates = soup.find('td', class_='rates').text.strip()
    except:
      rates = ''

    try:
      overview_title = soup.find('h2', class_='title-m', string='Overview')
      overview_text = overview_title.find_next_sibling('p').text.strip()
    except:
      overview_text = ''

    try:
      year_built = soup.find('span', string='Year Built:').next_sibling.strip()
    except:
      year_built = ''

    try:
      year_renovated = soup.find('span', string='Year Last Renovated:').next_sibling.strip()
    except:
      year_renovated = ''

    try:
      num_floors = soup.find('span', string='Number of Floors:').next_sibling.strip()
    except:
      num_floors = ''

    try:
      chain = soup.find('span', string='Chain:').find_next_sibling('a').text.strip()
    except:
      chain = ''
    try:
      chain_website = soup.find('span', string='Chain Website:').find_next_sibling('a').get('href')
    except:
      chain_website = ''

    try:
      discounts_offered_label = soup.find('span', class_='label', string='Discounts offered:')
      discounts_offered_list = [li.text.strip() for li in discounts_offered_label.find_next('ul').find_all('li')]
      discounts_offered = ", ".join(discounts_offered_list)
    except:
      discounts_offered = ''

    try:
      image_links = soup.find_all('a', attrs={'data-image': True})

      image_urls = [link['data-image'] for link in image_links]

      image_urls = ',\n'.join([link['data-image'] for link in image_links])
    except:
      image_urls = ''

    data = {
    "Link":link,
    "Name": name,
    "Address": address,
    "Phone": phone,
    "Fax": fax,
    "Hotel Email": hotel_email,
    "Hotel Website": hotel_website,
    "Hotel Classification": classification,
    "Commission": commission,
    "Rooms": rooms,
    "Rates": rates,
    "Overview Text": overview_text,
    "Year Built": year_built,
    "Year Last Renovated": year_renovated,
    "Number of Floors": num_floors,
    "Chain": chain,
    "Chain Website": chain_website,
    "Discounts offered": discounts_offered,
    "Image URLs": image_urls
}
    print(name)
    global_data_list.append(data)
    # return data

    # print(data)
    # print("Address", address)
    # print("Phone:", phone)
    # print("Fax:", fax)
    # print("Hotel Email:", hotel_email)
    # print("Hotel Website:", hotel_website)
    # print("Hotel Classification:", classification)
    # print("Commission:", commission)
    # print("Rooms:", rooms)
    # print("Rates:", rates)
    # print("Overview Text:", overview_text)
    # print("Year Built:", year_built)
    # print("Year Last Renovated:", year_renovated)
    # print("Number of Floors:", num_floors)
    # print("Chain:", chain)
    # print("Chain Website:", chain_website)
    # print("Discounts offered:", discounts_offered)
    # print("Image URLs",image_urls)


def scrape_hotels():
    global global_data_list
    # moderate_tourist_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,Moderate-Tourist-Class?pg=' #1-31
    # tourist_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,Tourist-Class?pg=' #1-1591
    # superior_tourist_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,Superior-Tourist-Class?pg=' #1-3819
    # moderate_first_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,Moderate-First-Class?pg=' #1-2225
    # limited_service_first_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,Limited-Service-First-Class?pg=' #1-229
    # first_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,First-Class?pg=' #1-2954
    superior_first_class = 'https://www.travelweekly.com/Hotels/Hotel-Search,Superior-First-Class?pg=' #1-1555
    # moderate_deluxe = 'https://www.travelweekly.com/Hotels/Hotel-Search,Moderate-Deluxe?pg=' #1-273
    # deluxe = 'https://www.travelweekly.com/Hotels/Hotel-Search,Deluxe?pg=' #1-297
    # superior_deluxe = 'https://www.travelweekly.com/Hotels/Hotel-Search,Superior-Deluxe?pg=' #1-9


    #base_url = 'https://www.travelweekly.com/Hotels/Berlin?pg='
    start_page =126 #126
    end_page = 500 #1555
    try:
      # with ThreadPoolExecutor(max_workers=8) as executor:
      #   futures = []

      for page_num in range(start_page, end_page + 1):
        page_link = f"{superior_first_class}{page_num}"
        print(f"Currently Scraping Page {page_num}/{end_page}")

        response = scraper.get(page_link)
        soup = BeautifulSoup(response.text, 'lxml')
        hotel_results = soup.find_all("div", class_="result")
        for hotel in hotel_results:
          hotel_link = hotel.find("a", class_="title")["href"]
          full_hotel_link = f'https://www.travelweekly.com{hotel_link}'
          get_hotels_details(full_hotel_link)
            # future = executor.submit(get_hotels_details,full_hotel_link)
            # futures.append(future)

        # for future in futures:
        #   try:
        #     result = future.result()
        #     if result:
        #       global_data_list.append(result)
          # except Exception as e:
          #   print(f"Error processing task: {str(e)}")

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Stopping gracefully.")
        asyncio.run(send_telegram_message('Received KeyboardInterrupt. Stopping gracefully'))
    except Exception as e:
        print(f"Error while trying to find the seller item element: {str(e)}")
        asyncio.run(send_telegram_message(f'Error happened {e}'))

    finally:
      global_df = pd.DataFrame(global_data_list)
      output_file = 'travel_weekly.xlsx'
      writer = pd.ExcelWriter(output_file,
                        engine='xlsxwriter',
                        engine_kwargs={'options': {'strings_to_urls': False}})
      global_df.to_excel(writer, index=False)
      writer.close()

      asyncio.run(send_file_to_telegram(output_file))
      asyncio.run(send_telegram_message(f'{superior_first_class} Finished Page {start_page}-{end_page}'))


scrape_hotels()