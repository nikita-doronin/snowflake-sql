import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
    """Choose the fruits you want in your custom Smoothie!"""
)

# The Select Box:
# option = st.selectbox(
#     'What is your favorite fruit?',
#     ('Banana', 'Strawberries', 'Peaches'))

# st.write('Your favorite fruit is:', option)

name_on_rorder = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_rorder)

# Display the Fruit Options List in Your Streamlit in Snowflake (SiS) App. 
session = get_active_session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
st.dataframe(data=my_dataframe, use_container_width=True)

# Miltiple Select:
ingredients_list = st.multiselect('Choose up to 5 ingredients:', my_dataframe, max_selections=5)

if ingredients_list:
    # st.write(ingredients_list) # to show a selected resilt
    # st.text(ingredients_list) # to show a list

    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
        
    # st.write(ingredients_string) # to show a selected resilt

    # Build a SQL Insert Statement to insert the ingredients into the orders table:
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
                values ('""" + ingredients_string + """', '"""+name_on_rorder+"""')"""
    
    # st.write(my_insert_stmt)
    # st.stop() # troubleshooting - stop the app at this point

    # # Show that an order has been placed (in this case the ingredients have been inserted such
    # # a new order into the orders table every time the ingredients are selected and this is not correct):
    # if ingredients_string:
    #     session.sql(my_insert_stmt).collect()
    #     st.success('Your Smoothie is ordered!', icon="✅")

    # Create the order button:
    time_to_insert = st.button('Submit Order')

    # Show that an order has been placed:
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
