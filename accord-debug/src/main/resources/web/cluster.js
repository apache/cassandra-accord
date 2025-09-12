/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { createApp } = Vue;

createApp({
    components: {
        'transaction-widget': window.TransactionWidgetComponent,
        'navigation-widget': window.NavigationWidgetComponent,
        'host-selector': window.HostSelectorComponent
    },
    data() {
        return {
            nodes: [],
            healthStatus: null,
            loading: false,
            error: null,
            intersectionObserver: null,
            searchFilter: '',
            selectedRange: null,
            selectedTransaction: null,
            showTransactionPopup: false,
            showCommandsForKeyPopup: false,
            currentNodeId: null,
            currentStoreId: null,
            transactionBreadcrumbs: [],
            commandsForKeyData: null
        };
    },
    mounted() {
        this.init();
        // Add keyboard event listener for escape key
        document.addEventListener('keydown', this.handleKeyDown);
    },
    methods: {
        async init() {
            await this.discoverNodes();
        },

        async discoverNodes() {
            this.loading = true;
            this.error = null;
            
            try {
                // Get nodes with stores already included
                const nodesResponse = await fetch('/hosts');
                if (!nodesResponse.ok) {
                    throw new Error('Failed to fetch nodes');
                }
                const nodes = (await nodesResponse.json()).data;
                if (!nodes || nodes.length === 0) {
                    this.error = 'No nodes found. Make sure nodes are registered with the debug server.';
                    return;
                }
                
                // Transform nodes to include transaction state
                const discoveredNodes = nodes.map(node => ({
                    id: node.id,
                    stores: node.stores.map(store => ({
                        storeId: store.storeId,
                        ranges: store.ranges,
                        transactions: [],
                        loading: false,
                        loaded: false,
                        error: null
                    }))
                }));

                this.nodes = discoveredNodes.sort((a, b) => a.id - b.id);
                
                if (this.nodes.length === 0) {
                    this.error = 'No accessible nodes found.';
                } else {
                    // Don't load transactions immediately - they'll be loaded lazily when visible
                    this.setupIntersectionObserver();
                }
            } catch (error) {
                this.error = 'Failed to discover nodes: ' + error.message;
                console.error('Node discovery error:', error);
            } finally {
                this.loading = false;
            }
        },

        async loadStoreTransactions(nodeId, store) {
            console.log(`Node ${nodeId} store ${store.storeId} loading lazily`);
            store.loading = true;
            store.error = null;
            
            try {
                const response = await fetch(`/hosts/${nodeId}/stores/${store.storeId}/transactions`);
                const data = (await response.json()).data;
                console.log(data)
                if (response.ok) {
                    store.transactions = data || [];
                    store.loaded = true;
                } else {
                    store.error = data.error || 'Failed to load transactions';
                }
            } catch (error) {
                store.error = 'Error: ' + error.message;
                console.error(`Error loading transactions for node ${nodeId}, store ${store.storeId}:`, error);
            } finally {
                store.loading = false;
            }
        },

        setupIntersectionObserver() {
            // Create intersection observer to lazy load transactions
            this.intersectionObserver = new IntersectionObserver((entries) => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        const storeElement = entry.target;
                        const nodeId = storeElement.dataset.nodeId;
                        const storeId = parseInt(storeElement.dataset.storeId);

                        // Find the store object and load transactions if not already loaded
                        const node = this.nodes.find(n => n.id === nodeId);
                        if (node) {
                            const store = node.stores.find(s => s.storeId === storeId);
                            if (store && !store.loaded && !store.loading) {
                                this.loadStoreTransactions(nodeId, store);
                                // Stop observing this element
                                this.intersectionObserver.unobserve(storeElement);
                            }
                        }
                    }
                });
            }, {
                root: null, // Use viewport as root
                rootMargin: '50px', // Load 50px before entering viewport
                threshold: 0.1 // Trigger when 10% visible
            });
            
            // Wait for DOM update then observe all store elements
            this.$nextTick(() => {
                this.observeStoreElements();
            });
        },

        observeStoreElements() {
            const storeElements = document.querySelectorAll('.command-store');
            storeElements.forEach(element => {
                this.intersectionObserver.observe(element);
            });
        },

        getFilteredTransactions(transactions) {
            if (!this.searchFilter) {
                return transactions;
            }
            
            const filter = this.searchFilter.toLowerCase();
            return transactions.filter(txn => {
                // Search in transaction ID
                if (txn.txnId && txn.txnId.toLowerCase().includes(filter)) {
                    return true;
                }
                
                // Search in save status
                if (txn.saveStatus && txn.saveStatus.toLowerCase().includes(filter)) {
                    return true;
                }
                
                // Search in other fields if they exist
                if (txn.durability && txn.durability.toLowerCase().includes(filter)) {
                    return true;
                }
                
                if (txn.executeAt && txn.executeAt.toLowerCase().includes(filter)) {
                    return true;
                }
                
                if (txn.participants && txn.participants.toLowerCase().includes(filter)) {
                    return true;
                }
                
                return false;
            });
        },

        async refreshAll() {
            await this.discoverNodes();
        },

        async toggleRangeFilter(range) {
            if (this.selectedRange === range) {
                await this.clearRangeFilter();
            } else {
                this.selectedRange = range;
                await this.applyRangeFilter(range);
            }
        },

        async clearRangeFilter() {
            this.selectedRange = null;
            // Reload all nodes without filter
            await this.discoverNodes();
        },

        async applyRangeFilter(range) {
            this.loading = true;
            this.error = null;

            try {
                // Get nodes filtered by range on server side
                const nodesResponse = await fetch(`/hosts?range=${encodeURIComponent(range)}`);
                if (!nodesResponse.ok) {
                    throw new Error('Failed to fetch filtered nodes');
                }
                const nodesWithStores = (await nodesResponse.json()).data;
                
                // Transform nodes to include transaction state
                const discoveredNodes = nodesWithStores.map(nodeWithStores => ({
                    id: nodeWithStores.id,
                    stores: nodeWithStores.stores.map(store => ({
                        storeId: store.storeId,
                        ranges: store.ranges,
                        transactions: [],
                        loading: false,
                        loaded: false,
                        error: null
                    }))
                }));

                this.nodes = discoveredNodes.sort((a, b) => a.id - b.id);
                
                if (this.nodes.length === 0) {
                    this.error = `No nodes found with range: ${range}`;
                } else {
                    // Setup intersection observer for lazy loading
                    this.setupIntersectionObserver();
                }
            } catch (error) {
                this.error = 'Failed to filter by range: ' + error.message;
                console.error('Range filter error:', error);
            } finally {
                this.loading = false;
            }
        },

        getFilteredNodes() {
            if (!this.searchFilter)
                return this.nodes;
            const res = [];
            for (const node of this.nodes) {
                const stores = this.getFilteredStores(node.stores);
                if (stores.length > 0) {
                    const copy = {...node};
                    copy.stores = stores;
                    res.push(copy);
                }
            }
            return res;
        },

        getFilteredStores(stores) {
            if (!this.searchFilter)
                return stores;

            const res = [];
            for (const store of stores) {
                const copy = {...store};
                if (!store.loaded)
                {
                    res.push(copy);
                    continue;
                }
                const transactions = this.getFilteredTransactions(store.transactions);
                if (transactions.length > 0) {

                    copy.transactions = transactions;
                    res.push(copy);
                }
            }
            return res;
        },

        showTransactionDetails(transaction, nodeId = null, storeId = null, isFromDependency = false) {
            this.selectedTransaction = transaction;
            this.currentNodeId = nodeId;
            this.currentStoreId = storeId;

            // Only add to breadcrumbs when navigating from dependencies
            if (isFromDependency && transaction) {
                this.transactionBreadcrumbs.push({
                    txnId: transaction.txnId,
                    transaction: transaction,
                    nodeId: nodeId,
                    storeId: storeId
                });
            } else if (!isFromDependency) {
                // Reset breadcrumbs for new transaction chains (when clicked directly from store)
                this.transactionBreadcrumbs = [{
                    txnId: transaction.txnId,
                    transaction: transaction,
                    nodeId: nodeId,
                    storeId: storeId
                }];
            }

            this.showTransactionPopup = true;
        },

        async showTransactionById(txnId) {
            if (!this.currentNodeId || this.currentStoreId === null) {
                alert('Cannot navigate to transaction: no current node/store context available.');
                return;
            }

            try {
                const response = await fetch(`/hosts/${this.currentNodeId}/stores/${this.currentStoreId}/transactions/${encodeURIComponent(txnId)}`);
                const data = (await response.json()).data;
                
                if (response.ok) {
                    this.showTransactionDetails(data, this.currentNodeId, this.currentStoreId, true);
                } else {
                    alert(`Transaction ${txnId} not found in node ${this.currentNodeId}, store ${this.currentStoreId}: ${data.error || 'Unknown error'}`);
                }
            } catch (error) {
                alert(`Error fetching transaction ${txnId}: ${error.message}`);
                console.error('Error fetching transaction:', error);
            }
        },

        navigateToBreadcrumb(breadcrumbIndex) {
            // Show the selected transaction
            const breadcrumb = this.transactionBreadcrumbs[breadcrumbIndex];
            this.selectedTransaction = breadcrumb.transaction;
            this.currentNodeId = breadcrumb.nodeId;
            this.currentStoreId = breadcrumb.storeId;
            
            // Truncate breadcrumbs to only include items up to the selected one
            this.transactionBreadcrumbs = this.transactionBreadcrumbs.slice(0, breadcrumbIndex + 1);
        },

        closeTransactionPopup() {
            this.showTransactionPopup = false;
            this.selectedTransaction = null;
            this.currentNodeId = null;
            this.currentStoreId = null;
            this.transactionBreadcrumbs = [];
        },

        async showCommandsForKey(routingKey) {
            if (!this.currentNodeId) {
                alert('Cannot load CommandsForKey: no current node context available.');
                return;
            }

            // Open the commands_for_key.html page in a new tab with the routing key and host
            const url = `/commands_for_key.html?host=${encodeURIComponent(this.currentNodeId)}&key=${encodeURIComponent(routingKey)}`;
            window.open(url, '_blank');
        },

        closeCommandsForKeyPopup() {
            this.showCommandsForKeyPopup = false;
            this.commandsForKeyData = null;
        },

        handleKeyDown(event) {
            if (event.key === 'Escape') {
                if (this.showCommandsForKeyPopup) {
                    this.closeCommandsForKeyPopup();
                } else if (this.showTransactionPopup) {
                    this.closeTransactionPopup();
                }
            }
        }
    },

    beforeUnmount() {
        // Clean up intersection observer
        if (this.intersectionObserver) {
            this.intersectionObserver.disconnect();
        }
        // Remove keyboard event listener
        document.removeEventListener('keydown', this.handleKeyDown);
    }
}).mount('#app');