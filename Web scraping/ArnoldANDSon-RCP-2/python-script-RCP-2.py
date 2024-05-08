# # **RCP G2**

import os
os.system('pip install bs4')
os.system(' pip install BeautifulSoup4')
os.system('pip install lxml')

# ###  **2. Import  necessary libraries**

import requests
from bs4 import BeautifulSoup
from lxml import etree
from lxml import html
import pandas as pd
import re
import datetime


# ###  **3. Start Coding**

# #### web scraping code:

# Define the URL of the collections page
collections_url = 'https://arnoldandson.com/collections'

# Send a GET request to the collections URL and retrieve the HTML content
result = requests.get(collections_url)

# Create a BeautifulSoup object to parse the HTML content
collections_soup = BeautifulSoup(result.content, 'html.parser')

# Convert the BeautifulSoup object to an lxml DOM object
dom = etree.HTML(str(collections_soup))

# Use XPath to target the <a> elements within the <li> elements on the collections page
collection_links = dom.xpath('/html/body/div/div/main/div/div[1]/div/ul/li//a')

# Extract the href attribute from each <a> element to get the collection URLs
collections = [link.get('href') for link in collection_links]


# Eliminate duplicate URLs by converting the list to a set and back to a list again
ls_collections = list(set(collections))
ls_collections

# Store the first collection URL in a variable
collection1_url = ls_collections[0]

# Send a GET request to the first collection URL
result1 = requests.get(collection1_url)

# Create a BeautifulSoup object to parse the HTML content of the first collection
collection1_soup = BeautifulSoup(result1.content, 'html.parser')

# Create empty lists to store the extracted information for each watch
link_list=[]
reference_numbers_list = []
brand_name_list=[]
parent_model_list = []
specific_model_list=[]
nickname_list=[]
markting_name_list=[]
img_url_list=[]
case_material_list=[]
diameter_list=[]
water_resistance_list = []
dial_list = []
bracelet_material_list=[]
bracelet_color_list=[]
calibre_list=[]
features_list=[]
descriptions_list=[]
type_list=[]
year_introduced_list=[]
style_list=[]
currency_list=[]
price_list=[]
made_in_list=[]
case_shape_list=[]
case_finish_list=[]
caseback_list=[]
between_lugs_list=[]
lug_to_lug_list=[]
case_thickness_list=[]
bezel_material_list=[]
bezel_color_list=[]
crystal_list=[]
weight_list=[]
numerals_list=[]
clasp_type_list=[]
movement_list=[]
power_reserve_list=[]
frequency_list=[]
jewels_list=[]
short_description_list=[]


# Iterate over each collection URL
for collection_url in ls_collections:
    # Send a GET request to the collection URL
    result = requests.get(collection_url)
    # Create a BeautifulSoup object to parse the HTML content of the collection
    collection_soup = BeautifulSoup(result.content, 'html.parser')
    # Find all <li> elements with the class 'w-full' that contain the watch items
    collection_items = collection_soup.find_all('li', attrs={'class': 'w-full'})

    # Iterate over each watch item in the collection
    for item in collection_items:
        # Find all <a> elements within the watch item
        links = item.find_all('a')
        # Iterate over each <a> element
        for link in links:
          # Extract the href attribute to get the watch URL
          link_list.append(link['href'])
          watch_url = link['href']
          # Send a GET request to the watch URL
          watch_result = requests.get(watch_url)
          # Create a BeautifulSoup object to parse the HTML content of the watch
          watch_soup = BeautifulSoup(watch_result.content, 'html.parser')

          # Find the Reference number
          reference = watch_soup.find('dt', text='Reference')
          if reference:
             reference_number = reference.find_next_sibling('dd').text
             reference_numbers_list.append(reference_number)
          else:
            reference_numbers_list.append("")


          # Find the brand name
          brand_element = watch_soup.find('small')
          # Extract the brand name by splitting the text using ' - ' as a separator
          # and taking the second part (index 1) after the split
          brand_name = brand_element.text.split(' - ')[1].strip() if brand_element else ''
          brand_name_list.append(brand_name)

          # Extract the parent model from the watch URL
          parent_model = watch_url.split('collections/')[1].split('/')[0]
          parent_model_list.append(parent_model)


          #Find  the Specific model
          spicific_model = watch_soup.select_one('h1.uppercase.font-bold.pt-12.text-2xl.pb-4')
          if spicific_model:
            # Extract the Specific model value
            spicific_model_value = spicific_model.text.strip()
            specific_model_list.append(spicific_model_value)
          else:
            specific_model_list.append("")

          # Extract the nickname from the watch URL
          link_result = requests.get(watch_url)
          link_content = link_result.content
          # Parse link content with lxml
          link_tree = html.fromstring(link_content)
          # Extract text using XPath expression
          text_content = link_tree.xpath('//*[@id="contentWrapper"]/div/div[1]/div/p')[0].text
          nickname_list.append(text_content)



          # Find the marketing name (limited edition with numbers)
          limited_edition_element = watch_soup.find('dt', text='Limited edition')
          if limited_edition_element:
            # Extract the limited edition number from the next sibling <dd> element
            limited_edition_number = limited_edition_element.find_next_sibling('dd').text.strip()
            limited_edition_label = limited_edition_element.text.strip()
            limited_editiion=limited_edition_label + ": " + limited_edition_number
            markting_name_list.append(limited_editiion)
          else:
            markting_name_list.append("")


          # Find the image URL
          images = watch_soup.find_all('img')
          for image in images:
            image_url = image['src']
            if '/thumbnail-cropped.jpg' in image_url:
              img_url_list.append(image_url)



          # Find the case material
          case_material = watch_soup.find('dt', text='Case')
          if case_material:
            case_value = case_material.find_next_sibling('dd').text
            case_material_list.append(case_value)
          else:
            case_material_list.append("")


          # Find Water resistance
          water_resistance = watch_soup.find('dt', text='Water resistance')
          if water_resistance:
            water_resistance_value = water_resistance.find_next_sibling('dd').text
            water_resistance_list.append(water_resistance_value)
          else:
            water_resistance_list.append("")

          # Find Dail color
          dial = watch_soup.find('dt', text='Dial')
          if dial:
            # Extract the Dial value
            dial_value = dial.find_next_sibling('dd').text.strip()
            dial_list.append(dial_value)
          else:
            dial_list.append("")


          # Find Bracele material
          bracelet_material = watch_soup.find('dt', text='Strap')
          if bracelet_material:
            bracelet_material_value = bracelet_material.find_next_sibling('dd').text
            bracelet_material_list.append(bracelet_material_value)
          else:
            bracelet_material_list.append("")



          # Find Calibre
          calibre = watch_soup.find('dt', text=lambda t: 'Calibre' in t)
          if calibre:
            calibre_text = calibre.get_text(separator=' ')
            calibre_list.append(calibre_text)
          else:
            calibre_list.append("")


          # Find Features -> listed under FUNCTIONS in the website
          features = watch_soup.find('dt', text='Functions')
          if features:
            features_value = features.find_next_sibling('dd').text
            features_list.append(features_value)
          else:
            features_list.append("")

          # Find Descripition -> case + dial + strap in the website
          case_des = watch_soup.find('dt', text='Case')
          dial_des = watch_soup.find('dt', text='Dial')
          strap_des = watch_soup.find('dt', text='Strap')
          description = ""
          if case_des:
            description += f"{case_des.text}: {case_des.find_next_sibling('dd').text}\n"
          if dial_des:
            description += f"{dial_des.text}: {dial_des.find_next_sibling('dd').text}\n"
          if strap_des:
            description += f"{strap_des.text}: {strap_des.find_next_sibling('dd').text}\n"
          if description.strip(): # Check if description is not empty
            descriptions_list.append(description.strip())
          else:
            descriptions_list.append("")

          # Finding the JEWELS, FREQUENCY , POWER RESERVE -> listed in the discover more page

          d_more = watch_soup.find('a', text='Discover more')
          if d_more:
            # Get the URL
            d_url = d_more['href']
            # Fetch content of the link
            d_result = requests.get(d_url)
            d_soup = BeautifulSoup(d_result.content, 'html.parser')

            # Extract "Jewels" information if available
            jewels = d_soup.find('dt', text='Jewels')
            if jewels:
              jewels_d = jewels.find_next_sibling('dd').text
              jewels_list.append(jewels_d)
            else:
              jewels_list.append('')

            # Extract "Power reserve" information if available
            power = d_soup.find('dt', text='Power reserve')
            if power:
              power_d = power.find_next_sibling('dd').text
              power_reserve_list.append(power_d)
            else:
              power_reserve_list.append('')


            # Extract "Frequency" information if available
            frequency = d_soup.find('dt', text='Frequency')
            if frequency:
              frequency_d = frequency.find_next_sibling('dd').text
              frequency_list.append(frequency_d)
            else:
              frequency_list.append('')

            # Extract "diameter" information if available
            diameter = d_soup.find('dt', text='Diameter')
            if diameter:
              diameter_d = diameter.find_next_sibling('dd').text
              diameter_list.append(diameter_d)
            else:
              diameter_list.append('')



# Find bracelet_material (by assigning the values of bracelet_material_list to bracelet_color_list
# since those values are both listed under STRAP so we just assigned it)

bracelet_color_list=bracelet_material_list


# #### creating empty lists for empty columns:

# Create empty lists for blank cells in these columns: (type, year_introduced, style, currency, price, made_in, case_shape, case_finish,
# caseback, between_lugs, lug_to_lug, case_thickness, bezel_material, bezel_color, crystal, weight, numerals, clasp_type, movement, short_description)


# Define a function to create a list of empty strings
def create_empty_list():
    return [""] * len(link_list)

# Create lists of empty strings using list comprehension
type_list = create_empty_list()
year_introduced_list = create_empty_list()
style_list = create_empty_list()
currency_list = create_empty_list()
price_list = create_empty_list()
made_in_list = create_empty_list()
case_shape_list = create_empty_list()
case_finish_list = create_empty_list()
caseback_list = create_empty_list()
between_lugs_list = create_empty_list()
lug_to_lug_list = create_empty_list()
case_thickness_list = create_empty_list()
bezel_material_list = create_empty_list()
bezel_color_list = create_empty_list()
crystal_list = create_empty_list()
weight_list = create_empty_list()
numerals_list = create_empty_list()
clasp_type_list = create_empty_list()
movement_list = create_empty_list()
short_description_list = create_empty_list()


# #### creating dictionaries for the dataframe


# The watches_info dictionary stores information about watches, with each key representing
# a specific field and its corresponding list as the value.

watches_info={
    'reference_number': reference_numbers_list,
    'watch_URL': link_list,
    'type': type_list,
    'brand': brand_name_list,
    'year_introduced': year_introduced_list,
    'parent_model': parent_model_list,
    'specific_model': specific_model_list,
    'nickname': nickname_list,
    'marketing_name': markting_name_list,
    'style': style_list,
    'currency':currency_list,
    'price': price_list,
    'image_URL': img_url_list,
    'made_in': made_in_list,
    'case_shape': case_shape_list,
    'case_material': case_material_list,
    'case_finish': case_finish_list,
    'caseback': caseback_list,
    'diameter': diameter_list,
    'between_lugs':between_lugs_list,
    'lug_to_lug': lug_to_lug_list,
    'case_thickness': case_thickness_list,
    'bezel_material': bezel_material_list,
    'bezel_color': bezel_color_list,
    'crystal': crystal_list,
    'water_resistance': water_resistance_list,
    'weight': weight_list,
    'dial_color': dial_list,
    'numerals': numerals_list,
    'bracelet_material':bracelet_material_list,
    'bracelet_color': bracelet_color_list,
    'clasp_type': clasp_type_list,
    'movement':movement_list,
    'calibre': calibre_list,
    'power_reserve': power_reserve_list,
    'frequency': frequency_list,
    'jewels': jewels_list,
    'features': features_list,
    'description': descriptions_list,
    'short_description':short_description_list

}


# ### **4. Creating DataFrame**


# Create a pandas DataFrame named 'df' using the 'watches_info' dictionary as the data source.
df = pd.DataFrame(watches_info)



#Display the DataFrame
df


# ### **4. Data Cleaning**

#case_material
# Apply regular expression and cleaning logic directly to the 'case_material' column
df['case_material'] = df['case_material'].apply(lambda x: re.sub(r'\d+(\.\d+)?\s*mm', '', x).strip(', '))

#diameter
# Strip any leading or trailing whitespace from the values in the 'diameter' column
df['diameter'] = df['diameter'].str.strip()

# Remove any 'mm' suffix from the values in the 'diameter' column
df['diameter'] = df['diameter'].str.replace('mm', '')

# Add ' mm' suffix to all values in the 'diameter' column and fill empty values
df['diameter'] = df['diameter'].apply(lambda x: f"{float(x.strip()):.2f} mm" if x.strip() else '')

#power_reserve
# Define a function to clean the power reserve values
def clean_power_reserve(value):
    # Remove 'h' and 'hours' suffixes, extract numeric value
    match = re.match(r'(\d+)\s*(?:hours?|h)?', value, re.IGNORECASE)
    if match:
        return f"{match.group(1)} hours"
    else:
        return ''

# Apply the cleaning function to the 'power_reserve' column
df['power_reserve'] = df['power_reserve'].apply(clean_power_reserve)

#'water_resistance'
# Define a function to clean the water resistance values
def clean_water_resistance(value):
    # Search for numeric values followed by 'meters'
    match = re.search(r'(\d+)\s*meters?', value, re.IGNORECASE)
    if match:
        # Extract the numeric value
        meters_value = float(match.group(1))
        # Convert meters to ATM using the conversion factor
        atm_value = meters_value / 10.19716212977928
        # Return the converted value with the string "ATM" appended
        return f"{round(atm_value, 2)} ATM"
    else:
        return ''

# Apply the cleaning function to the 'water_resistance' column
df['water_resistance'] = df['water_resistance'].apply(clean_water_resistance)


# ### **5. Saving the CSV file**


# Generate timestamp string with specified format
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")


# transfer the DataFrame 'df' to a CSV file
df.to_csv(f"volume/Arnold&Son_{timestamp}.csv", index=False)