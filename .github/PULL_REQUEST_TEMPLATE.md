<!--
*Thank you for contributing to Apache Flink AWS Connectors - we are happy that you want to help us improve our Flink connectors. To help the community review your contribution in the best possible way, please go through the checklist below, which will get the contribution into a shape in which it can be best reviewed.*

## Contribution Checklist

- The name of the pull request should correspond to a [JIRA issue](https://issues.apache.org/jira/projects/FLINK/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no JIRA issue.
- Commits should be in the form of "[FLINK-XXXX][component] Title of the pull request", where [FLINK-XXXX] should be replaced by the actual issue number. 
    Generally, [component] should be the connector you are working on.
    For example: "[FLINK-XXXX][Connectors/Kinesis] XXXX" if you are working on the Kinesis connector or "[FLINK-XXXX][Connectors/AWS] XXXX" if you are working on components shared among all the connectors.
- Each pull request should only have one JIRA issue.
- Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.
-->

## Purpose of the change

*For example: Implements the Table API for the Kinesis Source.*

## Verifying this change

Please make sure both new and modified tests in this PR follows the conventions defined in our code quality guide: https://flink.apache.org/contributing/code-style-and-quality-common.html#testing

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
- *Added integration tests for end-to-end deployment*
- *Added unit tests*
- *Manually verified by running the Kinesis connector on a local Flink cluster.*

## Significant changes
*(Please check any boxes [x] if the answer is "yes". You can first publish the PR and check them afterwards, for convenience.)*
- [ ] Dependencies have been added or upgraded
- [ ] Public API has been changed (Public API is any class annotated with `@Public(Evolving)`)
- [ ] Serializers have been changed
- [ ] New feature has been introduced
  - If yes, how is this documented? (not applicable / docs / JavaDocs / not documented)
