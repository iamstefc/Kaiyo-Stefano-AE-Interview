# Stefano-AE-Interview
An employer is an online marketplace committed to great design, exceptional customer care, and a more sustainable planet
Currently developing...

## Installing this Project

#### Basic Setup

This will get you set up to connect to the data warehouse in your local
environment and install the required packages for developing with this dbt
project.

1. Open up your project
2. Fill out your `profiles.yml` with your database credentials

If using conda:
1. `conda env create -f environment.yml`
2. `conda activate dbt`
3. `dbt debug`

If using pip:
1. `pip install dbt-core==1.3.1 dbt-redshift==1.3`
2. `dbt debug`
