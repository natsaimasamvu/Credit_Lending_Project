import logging

# Create or get a logger
logger = logging.getLogger("Data_Pipeline")

# Set level of logger
logger.setLevel(logging.INFO)

# Create a file handler
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(handler)
