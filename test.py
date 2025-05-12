import asyncio

from src.models.database_models.db_session import SessionLocal
from src.services.kafka_service.consumer import KafkaConsumer
from src.core.secrets import KafkaCredentials


# STEP 1: Create mock message object
class DummyMsg:
    def topic(self):
        return KafkaCredentials.AI_INTERACTION_TOPIC  # or hardcoded "ai-interaction-topic"

    def value(self):
        return mock_json  # use your full mock_json here


mock_json = {
    "transcriptID": "dce45617-45f8-4f9a-8408-8c04d7dbdb1d",
    "prospectiveRetailUserID": "9ff5855c-0d30-4ccd-94ef-9aa585af6154",
    "advisorID": "1f7c1ff9-352a-444b-873d-bdbfdad8b362",
    "sourceTopic": "wp-dev-1.transcript-file-response.eu-west-1",
    "phaseType": "FactFind",
    "sourceType": "Transcript",
    "sectionType": "PersonalDetails",
    "transcriptData": {
        "personalDetails": {
            "clientDetails": {
                "title": "Mr",
                "firstName": "John",
                "middleName": None,
                "lastName": "Newman",
                "gender": "Male",
                "telephoneNumber": None,
                "address": [
                    {
                        "addressType": None,
                        "addressLine1": "Winchester",
                        "addressLine2": "Hampshire",
                        "addressLine3": None,
                        "addressLine4": None,
                        "addressLine5": None,
                        "town": None,
                        "county": "Hampshire",
                        "country": "United Kingdom",
                        "postcode": None
                    }
                ],
                "dateOfBirth": "1971-08-14",
                "countryOfBirth": "United Kingdom",
                "placeOfBirth": None,
                "maritalStatus": "Married",
                "nationalInsuranceNumber": None,
                "nationality": "British",
                "citizenship": "British Citizen",
                "isUsCitizen": False,
                "taxResidency": [
                    {
                        "isPrimary": True,
                        "taxResidencyType": None,
                        "taxResidentCountry": "United Kingdom",
                        "taxIdentifierNumber": None,
                        "reason": None
                    }
                ],
                "employment": [
                    {
                        "employmentStatus": "Employed (Full-Time)",
                        "employerName": "Logistics Company",
                        "employmentType": "Full-Time",
                        "jobTitle": "Commercial Director",
                        "jobDescription": "Commercial Director",
                        "startDate": "2014",
                        "endDate": None,
                        "department": None,
                        "workLocation": None
                    }
                ]
            },
            "partnerDetails": {
                "title": "Mrs",
                "firstName": "Tara",
                "middleName": None,
                "lastName": "Newman",
                "gender": "Female",
                "telephoneNumber": None,
                "address": [
                    {
                        "addressType": None,
                        "addressLine1": "Winchester",
                        "addressLine2": "Hampshire",
                        "addressLine3": None,
                        "addressLine4": None,
                        "addressLine5": None,
                        "town": None,
                        "county": "United Kingdom",
                        "country": "United Kingdom",
                        "postcode": None
                    }
                ],
                "dateOfBirth": "1974-05-09",
                "countryOfBirth": "United Kingdom",
                "placeOfBirth": None,
                "maritalStatus": "Married",
                "nationalInsuranceNumber": None,
                "nationality": "British",
                "citizenship": "British citizen",
                "isUsCitizen": False,
                "taxResidency": [
                    {
                        "isPrimary": True,
                        "taxResidencyType": None,
                        "taxResidentCountry": "United Kingdom",
                        "taxIdentifierNumber": None,
                        "reason": None
                    }
                ],
                "employment": [
                    {
                        "employmentStatus": "Employed (Part-Time)",
                        "employerName": "NHS",
                        "employmentType": "Part-time",
                        "jobTitle": "Senior speech therapist",
                        "jobDescription": "Senior speech therapist",
                        "startDate": None,
                        "endDate": None,
                        "department": None,
                        "workLocation": None
                    }
                ]
            },
            "dependents": [
                {
                    "firstName": None,
                    "middleName": None,
                    "lastName": None,
                    "relationship": "Child",
                    "maritalStatus": None,
                    "isFinanciallyDependent": "Yes",
                    "gender": "",
                    "dateOfBirth": None,
                    "nameOfSchool": None,
                    "costOfSchoolCurrency": "GBP",
                    "costOfSchoolAmount": 750.0,
                    "dateOnWhichDependentStartedSchool": None,
                    "dateOfGraduation": None
                }
            ],
            "topicDetails": {
                "topicSummary": "markdown\n### Client Information Extracted from the Conversation\n\n**Q1: Full Name**\n- Full Name: John Newman\n\n**Q2: Gender**\n- Gender: Male\n\n**Q3: Title**\n- Title: Mr\n\n**Q4: Telephone Number**\n- Telephone Number: Not Provided\n\n**Q5: Address**\n- Address: Winchester, Hampshire, United Kingdom\n\n**Q6: Date of Birth, Country of Birth, Place of Birth**\n- Date of Birth: 1971-08-14\n- Country of Birth: United Kingdom\n- Place of Birth: Not Mentioned\n\n**Q7: Marital Status, National Insurance Number, Nationality, Citizenship Details**\n- Marital Status: Married\n- National Insurance Number: Not Provided\n- Nationality: British\n- Citizenship Details: British Citizen\n\n**Q8: Employment Details**\n- Employment Status: Employed (Full-Time)\n- Employer Name: Logistics Company\n- Employment Type: Full-Time\n- Job Description: Commercial Director\n- Job Title: Commercial Director\n- Job Start Date: 2014\n- Job End Date: Not Mentioned\n- Work Location: Not Mentioned\n- Department: Not Mentioned\n\n**Q9: Tax Residency Details**\n- Primary Resident: Yes\n- Country of Residency: United Kingdom\n- Tax Identifier Number: Not Provided\n\n**Q10: Employment Status ENUM**\n- Employment Status: Employed (Full-Time)\n\n**Q11: Marital Status ENUM**\n- Marital Status: Married\n\n**Q12: Residency Type ENUM**\n- Residency Type: CRS Reporting (Common Reporting Standard)\n\n**Q13: Country of Birth ENUM**\n- Country of Birth: United Kingdom\n\n### Partner Information Extracted from the Conversation\n\n**Q1: Full Name**\n- Full Name: Tara Newman\n\n**Q2: Gender**\n- Gender: Female\n\n**Q3: Title**\n- Title: Mrs\n\n**Q4: Telephone Number**\n- Telephone Number: Not Provided\n\n**Q5: Address**\n- Address: Winchester, Hampshire, United Kingdom\n\n**Q6: Date of Birth, Country of Birth, Place of Birth**\n- Date of Birth: 1974-05-09\n- Country of Birth: United Kingdom\n- Place of Birth: Not Mentioned\n\n**Q7: Marital Status, National Insurance Number, Nationality, Citizenship Details**\n- Marital Status: Married\n- National Insurance Number: Not Provided\n- Nationality: British\n- Citizenship Details: British Citizen\n\n**Q8: Employment Details**\n- Employment Status: Employed (Part-Time)\n- Employer Name: NHS\n- Employment Type: Part-Time\n- Job Description: Senior Speech Therapist\n- Job Title: Senior Speech Therapist\n- Job Start Date: Over 10 years ago\n- Job End Date: Not Mentioned\n- Work Location: Not Mentioned\n- Department: Not Mentioned\n\n**Q9: Tax Residency Details**\n- Primary Resident: Yes\n- Country of Residency: United Kingdom\n- Tax Identifier Number: Not Provided\n\n**Q10: Employment Status ENUM**\n- Employment Status: Employed (Part-Time)\n\n**Q11: Marital Status ENUM**\n- Marital Status: Married\n\n**Q12: Residency Type ENUM**\n- Residency Type: CRS Reporting (Common Reporting Standard)\n\n**Q13: Country of Birth ENUM**\n- Country of Birth: United Kingdom\n\n### Dependent Information Extracted from the Conversation\n\n**Q1: Dependent Details**\n- **Name:** Not specified\n- **Relationship:** Child\n- **Date of Birth:** Not specified\n- **Age:** Not specified\n- **Marital Status:** Not specified\n- **Name of School:** Not specified\n- **Cost of School:** £750 per month\n- **Cost of School Currency:** Not specified\n- **Start Date:** Not specified\n- **Graduation Date:** Not specified\n- **Gender:** Not specified\n\n**Q2: Dependent Gender ENUM**\n- Gender ENUMs: \"\"\n\n**Q3: Dependent Relationship ENUM**\n- Relationship ENUMs: Child\n",
                "topicID": None,
                "topicStartTime": None,
                "topicEndTime": None,
                "conversations": None
            }
        }
    }
}


async def run():
    db = SessionLocal()
    consumer = KafkaConsumer(db=db)
    await consumer.process_message(DummyMsg())


# STEP 4: Execute
if __name__ == "__main__":
    asyncio.run(run())