import requests
import streamlit as st
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
    """Choose the fruits you want in your custom Smoothie!"""
)

name_on_rorder = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_rorder)

# Display the Fruit Options List in Your Streamlit in Snowflake (SniS) App.
cnx = st.connection("snowflake")
session = cnx.session()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
# st.dataframe(data=my_dataframe, use_container_width=True)

# Convert the Snowpark Dataframe to a Pandas Dataframe so we can use the LOC function
pd_df=my_dataframe.to_pandas()
# st.dataframe(pd_df)

# Miltiple Select:
ingredients_list = st.multiselect('Choose up to 5 ingredients:', my_dataframe, max_selections=5)

if ingredients_list:
    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '

        # search_on=pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'search_on'].iloc[0]
        # st.write('The search value for ', fruit_chosen,' is ', search_on, '.')

        st.subheader(fruit_chosen + ' Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + fruit_chosen)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)

    # Build a SQL Insert Statement to insert the ingredients into the orders table:
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
                values ('""" + ingredients_string + """', '"""+name_on_rorder+"""')"""

    # st.stop() # troubleshooting - stop the app at this point
  
    # Create the order button:
    time_to_insert = st.button('Submit Order')

    # Show that an order has been placed:
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
