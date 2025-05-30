.PHONY: tables


tables:
	awslocal dynamodb create-table \
		--table-name 'TEST_TABLE' \
		--attribute-definitions \
			AttributeName=id,AttributeType=S \
		--key-schema \
			AttributeName=id,KeyType=HASH \
		--provisioned-throughput \
			ReadCapacityUnits=1,WriteCapacityUnits=1 \
	| tee
