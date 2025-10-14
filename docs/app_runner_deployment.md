# Deploying the FF Dashboard with AWS App Runner

This guide explains how to connect the GitHub repository to an App Runner service so the FF Dashboard is automatically built and deployed from source.

## 1. Prepare the repository

1. **Add a production configuration file** (if you have not already) that describes the App Runner secrets. Store the values in App Runner rather than committing them to the repo.
2. **Include the `apprunner.yaml` file at the repository root.** This file tells App Runner to use the managed Python 3.11 runtime, install the dependencies from `requirements.txt`, and run Streamlit with the correct flags (including disabling WebSocket compression). Make sure any updates to runtime behaviour are reflected in this file.

## 2. Authorize GitHub in App Runner

1. Open the [App Runner console](https://console.aws.amazon.com/apprunner/home).
2. Choose **Services → Create service**.
3. When asked for a source, pick **Source code repository** and click **Add new** (or select the GitHub connection that you already created).
4. Follow the prompts to authorize AWS App Runner to access the GitHub organization/repository that hosts this project.

## 3. Point App Runner to the repository

1. In **Repository**, select the GitHub repo that contains the FF Dashboard (for example, `your-org/ASP-FF-Dashboard`).
2. Choose the branch to deploy (commonly `main` or `production`).
3. Set the **Deployment trigger**. Use **Automatic** if you want App Runner to redeploy when new commits are pushed.

## 4. Configure the build & runtime

1. In **Build configuration**, choose **Use a configuration file** and keep the default path `apprunner.yaml` (or point to the location if you moved it). App Runner will read the build and run commands from this file instead of requiring a Dockerfile.
2. In **Runtime configuration**, leave the defaults—the start command, port, and environment variables are supplied by `apprunner.yaml`. You can override values here if you need to temporarily test a change.
3. Add environment variables that map to the secrets the app expects. A common pattern is to keep secret values in AWS Secrets Manager and inject them via environment variables.

## 5. Networking and IAM

* If the service needs to reach private AWS resources (DynamoDB tables in a VPC, private API Gateway endpoints, etc.), configure an App Runner VPC connector.
* Attach an IAM role with permissions for DynamoDB, Secrets Manager, FlightAware SQS/SNS, or other integrations.

## 6. First deployment

1. Review the summary and create the service.
2. App Runner will clone the repository, run the build commands from `apprunner.yaml`, and deploy the managed runtime.
3. Once the service status becomes **Running**, open the default domain shown in the console to verify that Streamlit loads.

## 7. Ongoing deployments

When **Automatic** deployment is enabled, App Runner will repeat the steps defined in `apprunner.yaml` whenever commits are pushed to the configured branch. You can also trigger a manual deployment from the console or via the AWS CLI if you need to redeploy without a new commit.

## Troubleshooting tips

* Use the **Logs** tab in the App Runner console to read the build and runtime logs. Streamlit logs appear in the runtime logs.
* Confirm that all required secrets are defined as environment variables. A missing secret is a common cause of runtime errors.
* If the build fails, double-check that the dependencies listed in `requirements.txt` (and any system packages they rely on) are installed correctly. You can add extra `pip install` or `apt-get` commands to the `apprunner.yaml` build step when needed.
* If outbound requests to FlightAware or email servers fail, ensure the App Runner service has the necessary egress (public internet or VPC routing) and IAM permissions.
* If you see the browser console report `WebSocket connection to 'wss://<app>.awsapprunner.com/_stcore/stream' failed`, double-check that the repository's `.streamlit/config.toml` and the Streamlit CLI arguments both keep `enableWebsocketCompression` set to `false`.
