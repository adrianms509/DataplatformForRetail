# Dataplatform for Retail

A high-level demo consisting of three components showcasing the Microsoft Platform for a Cross-Solution Area Demo focused on Fabric as the enabler for intelligence across your retail organization.


## Architecture

![Demo Architecture](./DemoOverview.png)

The demo consists of three parts:
* Store Manager Agent: A simple CoPilot-Studio Agent that supports employees in a retail store with answering questions about the inventory and other product related questions

* Fabric Data Agent & CoPilot in PowerBI: The data agent supplies data to the store manager agent and and example report can be used to query and modify the report in natural language

* Virtual Try On: AI-powered personalized shopping experience in which shoppers can create custom outfits by combining different pieces of clothing with GPT Image 1.5. The data is streamed into Fabric to enable Realtime insights into trends.


## Setup
To setup the demo for yourself, please request a teannt with CoPilotStudio licenses and at least one Azure Subscriptions or enable PAYGO billing for CoPilotStudio. Then follow the README.md files in the respective directories.

## Running the demo

### CoPilot Studio / Data Agent Prompts

In CoPilot Studio:
```
What is the product P0023 made of?
```

```
What other products do customers buy with this?
```

In the Data Agent Overview (optional):
```
How many jackets made of Polyester-Jacquard material are there in Cologne, broken down by size?
```

### CoPilot in PowerBI

Open the Sales Report in Edit more, paste this in CoPilot:

```
Create a sales performance dashboard with the following visuals: Total revenue and total orders as sums
a line chart of the average order value (AOV) by date
a bar chart of the sum of revenue by region and channel
a bar chart of sum of revenue by brand as well as a bar chart of sum of revenue per category
```

In the general Copilot for PowerBI:

```
how many purchases where made in december with a basket size larger than 3?
```

### Virtual Try On + RTI
1. Show the Virtual Try On
2. Show the Eventstream and quickly explain how data is transformed on the fly
3. Show the Realtime-Dashboard + Create an Activator to identify viral combinations