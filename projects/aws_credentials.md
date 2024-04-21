### AWS CLI configuration ad credential file settings

[https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)

```
aws configure list
aws configure list-profiles
aws configure import --csv file://credentials.csv
```

### AWS CLI supported environment variables

[https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_SESSION_TOKEN
- AWS_DEFAULT_REGION

```
aws configure set <parameter> <setting> --profile <profile>

parameters:
- aws_access_key_id
- aws_secret_access_key
- aws_session_token
```

aws configure set aws_access_key_id <AWS_ACCESS_KEY_ID> --profile udacity
aws configure set aws_secret_access_key <AWS_SECRET_ACCESS_KEY> --profile udacity
aws configure set aws_session_token <AWS_SESSION_TOKEN> --profile udacity
aws configure set region <REGION> --profile udacity
