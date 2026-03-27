import os

print("Running Bronze Layer...")
os.system("python scripts\\bronze_layer.py")

print("Running Silver Layer...")
os.system("python scripts\\silver_layer.py")

print("Running Gold Layer...")
os.system("python scripts\\gold_layer.py")

print("Pipeline completed successfully.")