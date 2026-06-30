# SkyFlow: Global Cross-Cloud Bulk Data Transfer Platform

![Service Architecture of the SkyFlow Platform](service-architecture.png)

## Overview
SkyFlow is a high-performance bulk data transfer platform built on a global cross-cloud network, designed to address the key pain points of existing cloud-based data transfer solutions: high costs, single-cloud lock-in, and poor scalability across providers.

The platform is optimized for cloud-based analytics, storage, and media services, with core innovations in dynamic routing and elastic scaling to ensure stable, high-throughput data transfer even under resource saturation.

---

## Core Challenges Solved
Existing bulk data transfer solutions suffer from:
- **High bandwidth costs**: Lack of optimization for fixed-bandwidth subscription models (the most cost-effective option for file transfer).
- **Single-cloud limitation**: Cannot scale seamlessly across multiple cloud providers.
- **Poor stability under saturation**: System bottlenecks and throughput drops when cloud resources approach full utilization.
- **Resource volatility**: Inability to adapt to dynamic changes in cloud resource availability.

---

## Key Technical Innovations

### 1. Lyapunov-based Universal Max-Weight Routing
- **Core Function**: Tackles cloud resource volatility and large-scale routing optimization.
- **Benefit**: Ensures system stability and high resource utilization even under saturated network links, maximizing the value of fixed-bandwidth subscription models.

### 2. Backpressure-aware Elastic Scaling
- **Core Function**: Dynamically adjusts cloud resources in real time as resources approach saturation.
- **Benefit**: Alleviates system bottlenecks and maintains smooth, high-throughput data flow during bulk transfers.

### 3. Cross-Cloud Architecture Foundation
SkyFlow's underlying architecture enables global cross-cloud deployment:
#### Two-Level Resource Abstraction
- **Virtual Machines (VMs)**: Fundamental execution unit for data forwarding and network measurement (e.g., `instance-20260202-081825`).
- **Nodes**: Grouped VMs mapping to cloud regions or availability zones (e.g., `europe-west4-b`), acting as the basic unit for resource management and scheduling.

#### Decoupled Control–Data Plane
- **Control Plane**: Deployed at node level, responsible for Lyapunov-based routing optimization and elastic scaling decisions.
- **Data Plane**: Deployed at VM level, running `data-plane` (resource metrics collection/network probing) and `data-proxy` (actual file forwarding/proxying) services.
- **Edge Client**: `client-plugin` manages file chunking and multi-path parallel transmission for bulk data at the system edge.

#### Horizontal State Synchronization
- Distributed state sync across control-plane instances eliminates centralized bottlenecks, enabling independent routing decisions and enhancing robustness in large-scale deployments.

---

## Implementation Architecture Diagram
The end-to-end architecture for bulk data transfer is shown below:

![SkyFlow Bulk Data Transfer Architecture](implementation.png)

### Figure Caption
**SkyFlow bulk data transfer architecture**, illustrating the cross-cloud two-level resource abstraction (VMs and nodes) and decoupled control–data plane design. The control plane leverages Lyapunov-based Universal Max-Weight routing and backpressure-aware elastic scaling, while the data plane ensures high-throughput bulk data forwarding. Horizontal state synchronization enables resilient, global cross-cloud data transfer across major cloud providers.

---

## Performance Results (under Different Network Setups)
Deployed across multiple major cloud providers, SkyFlow delivers significant improvements over open-source tools and commercial services:
- **Cost Reduction**: Reduces bandwidth costs by up to x%.
- **Transfer Speed**: Achieves up to y× faster replication for bulk data.
- **Stability**: Maintains high utilization and throughput even under saturated network links.

---

## Key Workflows for Bulk Data Transfer
1. `rigel-client` chunks bulk files into segments and initiates transfer requests.
2. Control plane computes optimal routing paths via Lyapunov-based Universal Max-Weight algorithm.
3. Data plane reports real-time resource metrics to trigger backpressure-aware scaling if needed.
4. Control plane synchronizes global state across cloud regions for cross-provider routing.
5. `data-proxy` executes parallel bulk data forwarding across optimized paths.
6. Rate-limited gateways ensure stable throughput for large-scale transfers.