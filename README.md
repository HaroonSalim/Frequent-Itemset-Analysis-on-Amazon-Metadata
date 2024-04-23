# Apache-Kafka-BDA
BDA Assignment on Apache Kafka<br>
Project Name: Real-time Streaming Analytics Pipeline

# Team Members:

Haroon Salim<br>
Immad Shahid<br>
Ahmad Luqman<br>
# Overview:
This project implements a real-time streaming analytics pipeline for processing and analyzing data streams using Python and Kafka. The pipeline includes components for data preprocessing, frequent itemset mining algorithms (Apriori and PCY), and additional analysis using innovative approaches.

# Approach:

- Data Preprocessing: We preprocess the data using Python scripts to clean and format it for analysis. This includes removing duplicates, handling missing values, and tokenizing text fields if present.
- Streaming Pipeline Setup: We use Kafka as the messaging system to implement the streaming pipeline. The pipeline consists of a producer application that streams preprocessed data to a Kafka topic and multiple consumer applications that subscribe to this data stream.

# Frequent Itemset Mining:
- Apriori Algorithm: We implement the Apriori algorithm in one of the consumers using a sliding window approach to approximate frequent itemsets. <br>
- PCY Algorithm: We implement the PCY algorithm in another consumer, utilizing a hash table to count item pairs and reduce memory consumption. <br>
- Innovative Analysis: We perform sentiment analysis on product descriptions from the SQLite database containing Amazon Metadata. <br>

# Technologies/Libraries Used:

- Python 3.11.9 <br>
- Kafka <br>
- Confluent Kafka Python library (confluent_kafka) <br>
- NLTK (Natural Language Toolkit) for text processing <br>

# Installation:

**Python:** Install Python from the [official website](https://www.python.org). <br>
**Kafka:** Install Kafka by following the instructions on the [official website](https://kafka.apache.org). <br>

**Confluent Kafka Python library:** <br>
Install the confluent_kafka library using pip: <br>
pip install confluent_kafka

**NLTK:** (for text processing) Install NLTK using pip: <br>
pip install nltk

# Usage:

- Clone this repository to your local machine.
- Run the provided Python scripts for the producer and consumer applications.
- Follow the instructions in the scripts to configure Kafka and adjust parameters as needed.
- Explore the output in real time to gain insights from the streaming data.


# License:
This project is licensed under the MIT License. See the LICENSE file for details.

# Acknowledgements:
We would like to thank the open-source community for providing valuable resources and libraries that made this project possible.

# Team:
For any inquiries or feedback, please contact the amazing team behind this project: <br>
Haroon Salim [Github](https://github.com/HaroonSalim) <br>
Immad Shahid [Github](https://github.com/immadshahid) <br>
Ahmad Luqman [Github](https://github.com/ahmadluqman) <br>

