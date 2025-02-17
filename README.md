# Case 1 - SQL

## Credentials

Due to new Google Cloud policies, shared credentials in public repos are automatically disabled. To be able to query in the code, **DOWNLOAD the credentials.json file in [THIS LINK](https://drive.google.com/drive/u/0/folders/1RUpBViKoCOmAwgaNebg3rxVVx_SgJOUk) and place it in the script folder**

## Answers to the Case - APIs

The analysis for the Items searches are in the PDF.

There's a published Tableau dashboard in the Tableau Public. You can access in [THIS LINK](https://public.tableau.com/app/profile/helio.assakura/viz/ItemSearches-Case2/ItemSearches-Case2). There's also a `.twbx` in the Github Repo that can be imported in the Tableau Desktop.

The dashboard consists on a few graphs and tables that track the results from the extractions in the API. Due to free Tableau constraints, we had to download the data from BigQuery and convert to Excel file to import in Tableau.

## Code

There's a code that generate 2 CSVs with the API extractios: One named `items.csv` with the columns from the JSON responses, and one with `items_attributes.csv` with the attributes extracted from the `attributes` column.

### Creating a virtual environment

Prepare you environment by creating a virtualenv:

    python3 -m venv venv

Then you can add the environment binaries to you path running:

    source venv/bin/activate

To leave your venv, just run `deactivate`.

### Depedencies

After creating the venv, you should install the dependencies with:

    pip install -r requirements.txt

### Running the script

Run the script with:

    python3 get_items.py 

The script should use a JSON file with the credentials to access the API, and they are in the **credentials.json** file.

It generates 2 CSVs with the extracted data from the mercadolibre's API and inserts into tables in BigQuery.

One example of response can be found here: https://api.mercadolibre.com/sites/MLA/search?q=merlot&limit=50#json

## Files

There are extra files / folders:

1. **Item Searches - Case 2.twbx**: Tableau twbx file with the Dashboard and Datasources
2. **/images**: Contains the images of the Tableau dashboard
3. **items.csv**: Final CSV file with the items JSON response converted as a BigQuery table.
4. **items_attributes.csv**: Final CSV file with the items attributes column converted as a BigQuery table.
5. **PDF**: Final analysis for the Items data from the API.