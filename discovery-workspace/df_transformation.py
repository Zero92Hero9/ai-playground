import pandas as pd
import pyarrow.parquet as pq
import json
import time
import numpy as np

def process_record(record):
    record = json.loads(record)
    eventId = record['eventId']
    timestamp = record['timestamp']
    revisionId = record['revisionId']
    currency = record['currency']
    displayPriceIncludesFacilityFee = record['displayPriceIncludesFacilityFee']
    displayPriceIncludesServiceFeesAndTaxes = record['displayPriceIncludesServiceFeesAndTaxes']
    internetEventTicketLimit = record['internetEventTicketLimit']
    offers = record['offers']

    data = []
    for offer in offers:
        offerId = offer['offerId'] or np.nan
        featureSets = offer['featureSets'] or np.nan
        offerSets = offer['offerSets'] or np.nan
        isQAEnabled = offer['isQAEnabled'] or np.nan

        attributes = offer['attributes'] or np.nan

        limit = attributes['limit']
        exact_limit = limit['exact'] if limit else np.nan
        min_limit = limit['min'] if limit else np.nan
        max_limit = limit['max'] if limit else np.nan
        multiples_limit = limit['multiples'] if limit else np.nan

        cardTypes = attributes['cardTypes'] or np.nan
        unlockCode = attributes['unlockCode'] or None
        unlock_code_type = unlockCode['type'] if unlockCode else np.nan
        code = unlockCode['code'] if unlockCode else np.nan
        categoryId = unlockCode['categoryId'] if unlockCode else np.nan

        displayTexts = unlockCode['displayTexts'] if unlockCode else None
        displayText = displayTexts[0] if (displayTexts and len(displayTexts) > 0) else np.nan
        langCode = displayText['langCode'] if displayText is not np.nan else np.nan
        isDefaultLangCode = displayText['isDefaultLangCode'] if displayText is not np.nan else np.nan
        learnMoreInformationTitle = displayText['learnMoreInformationTitle'] if displayText is not np.nan else np.nan
        learnMoreInformationInstructionalText = displayText['learnMoreInformationInstructionalText'] if displayText is not np.nan else np.nan
        learnMoreInformationExternalLinkLabel = displayText['learnMoreInformationExternalLinkLabel'] if displayText is not np.nan else np.nan
        learnMoreInformationExternalLinkUrl = displayText['learnMoreInformationExternalLinkUrl'] if displayText is not np.nan else np.nan

        beginDate = unlockCode['beginDate'] if unlockCode else np.nan
        endDate = unlockCode['endDate'] if unlockCode else np.nan
        groupRef = unlockCode['groupRef'] if unlockCode else np.nan

        priceZones = offer['priceZones']
        for zone in priceZones:
            priceZone = zone['priceZone'] or np.nan
            areas = zone['areas'] or np.nan
            prices = zone['prices'] or np.nan
            rawXP = zone['rawXP'] or np.nan

            for price in prices:
                displayPrice = price['displayPrice'] or np.nan
                faceValue = price['faceValue'] or np.nan
                facilityFee = price['facilityFee'] or np.nan
                serviceCharge = price['serviceCharge'] or np.nan
                distanceCharge = price['distanceCharge'] or np.nan
                faceValueTax = price['faceValueTax'] or np.nan
                serviceChargeTax = price['serviceChargeTax'] or np.nan
                serviceChargeTax2 = price['serviceChargeTax2'] or np.nan
                priceFeatureSets = price['featureSets'] or np.nan
                

                data_record = {
                    "eventId": eventId,
                    "timestamp": timestamp,
                    "revisionId": revisionId,
                    "currency": currency,
                    "displayPriceIncludesFacilityFee": displayPriceIncludesFacilityFee,
                    "displayPriceIncludesServiceFeesAndTaxes": displayPriceIncludesServiceFeesAndTaxes,
                    "internetEventTicketLimit": internetEventTicketLimit,
                    "offerId": offerId,
                    "featureSets": featureSets,
                    "offerSets": offerSets,
                    "isQAEnabled": isQAEnabled,
                    "exact_limit": exact_limit,
                    "min_limit": min_limit,
                    "max_limit": max_limit,
                    "multiples_limit": multiples_limit,
                    "cardTypes": cardTypes,
                    "unlock_code_type": unlock_code_type,
                    "code": code,
                    "categoryId": categoryId,
                    "langCode": langCode,
                    "isDefaultLangCode": isDefaultLangCode,
                    "learnMoreInformationTitle": learnMoreInformationTitle,
                    "learnMoreInformationInstructionalText": learnMoreInformationInstructionalText,
                    "learnMoreInformationExternalLinkLabel": learnMoreInformationExternalLinkLabel,
                    "learnMoreInformationExternalLinkUrl": learnMoreInformationExternalLinkUrl,
                    "beginDate": beginDate,
                    "endDate": endDate,
                    "groupRef": groupRef,
                    "priceZone": priceZone,
                    "areas": areas,
                    "displayPrice": displayPrice,
                    "faceValue": faceValue,
                    "facilityFee": facilityFee,
                    "serviceCharge": serviceCharge,
                    "distanceCharge": distanceCharge,
                    "faceValueTax": faceValueTax,
                    "serviceChargeTax": serviceChargeTax,
                    "serviceChargeTax2": serviceChargeTax2,
                    "priceFeatureSets": priceFeatureSets,
                    "rawXP": rawXP
                }
                data.append(data_record)

    return data

def transform_data_to_df(input_data, batch_size=100):
    batch_dfs = []

    for i in range(0, len(input_data), batch_size):
        batch_start_time = time.time()
        batch_data = input_data[i:i + batch_size]

        # Process batch data
        batch_processed = [process_record(record) for record in batch_data]
        batch_flat = [item for sublist in batch_processed for item in sublist]

        # Convert to DataFrame
        batch_df = pd.DataFrame(batch_flat)
        batch_dfs.append(batch_df)

        batch_end_time = time.time()
        print(f"Batch {i // batch_size + 1} processed in {batch_end_time - batch_start_time} seconds")

    # Concatenate all batch DataFrames into a single DataFrame
    final_df = pd.concat(batch_dfs, ignore_index=True)

    return final_df


def load_from_parquet_convert_to_df(filename='events.parquet'):
    parquet_table = pq.read_table(filename)
    pandas_table = parquet_table.to_pandas()
    return pandas_table


def load_and_flatten(filename='data.json'):
    data = read_from_json(filename)
    transformed_data = transform_data_to_df(data)
    return transformed_data


def read_from_json(filename='data.json'):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

def save_df_to_csv(df, filepath='events.csv'):
    df.to_csv(filepath)


converted_df = load_from_parquet_convert_to_df()
content = converted_df['content']
transformed_df = transform_data_to_df(content, batch_size=1000)
save_df_to_csv(transformed_df)
print(transformed_df)