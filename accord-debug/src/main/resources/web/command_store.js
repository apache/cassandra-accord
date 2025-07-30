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
        'host-selector': window.HostSelectorComponent,
        'routing-key-widget': window.RoutingKeyWidgetComponent
    },
    data() {
        return {
            hosts: [],
            selectedHostId: new URLSearchParams(window.location.search).get('host') || '',
            selectedStoreId: new URLSearchParams(window.location.search).get('store') || '',
            selectedProperty: null,
            transactions: [],
            propertyFilteredTxnIds: [],
            redundantBeforeData: [],
            loading: false,
            error: null,
            searchFilter: ''
        };
    },
    computed: {
        selectedHost() {
            return this.hosts.find(host => host.id === this.selectedHostId) || null;
        },
        
        selectedStore() {
            if (!this.selectedHost || !this.selectedStoreId) return null;
            return this.selectedHost.stores.find(store => store.storeId.toString() === this.selectedStoreId.toString()) || null;
        },
        
        filteredTransactions() {
            let filtered = this.transactions;
            
            // Apply search filter
            if (this.searchFilter) {
                const filter = this.searchFilter.toLowerCase();
                filtered = filtered.filter(txn => {
                    return Object.values(txn).some(value => 
                        value && value.toString().toLowerCase().includes(filter)
                    );
                });
            }
            
            return filtered;
        }
    },
    mounted() {
        this.loadHosts();
    },
    methods: {
        async loadHosts() {
            this.loading = true;
            this.error = null;
            
            try {
                const response = await fetch('/hosts');
                if (!response.ok) {
                    throw new Error(`Failed to load hosts: ${response.statusText}`);
                }
                
                const result = await response.json();
                console.log('Hosts response:', result);
                
                this.hosts = result.data || [];
                
                // If host was in URL, handle initial selection
                if (this.selectedHostId) {
                    await this.handleHostChange();
                }
            } catch (error) {
                console.error('Error loading hosts:', error);
                this.error = error.message;
                this.hosts = [];
            } finally {
                this.loading = false;
            }
        },
        
        async handleHostChange() {
            this.selectedStoreId = '';
            this.transactions = [];
            this.selectedProperty = null;
            this.propertyFilteredTxnIds = [];
            
            if (!this.selectedHostId) return;
            
            // If store ID was in URL, try to select it
            const urlStoreId = new URLSearchParams(window.location.search).get('store');
            if (urlStoreId && this.selectedHost) {
                const store = this.selectedHost.stores.find(s => s.storeId.toString() === urlStoreId);
                if (store) {
                    this.selectedStoreId = urlStoreId;
                    await this.handleStoreChange();
                }
            }
        },
        
        async handleStoreChange() {
            this.transactions = [];
            this.selectedProperty = null;
            this.propertyFilteredTxnIds = [];
            this.redundantBeforeData = [];
            
            if (!this.selectedStoreId) return;
            
            await Promise.all([
                this.loadTransactions(),
                this.loadRedundantBefore()
            ]);
            
            // Update URL with current selection
            const url = new URL(window.location);
            url.searchParams.set('host', this.selectedHostId);
            url.searchParams.set('store', this.selectedStoreId);
            window.history.replaceState({}, '', url);
        },
        
        async loadTransactions() {
            if (!this.selectedHostId || !this.selectedStoreId) return;
            
            this.loading = true;
            this.error = null;
            
            try {
                const response = await fetch(`/hosts/${this.selectedHostId}/stores/${this.selectedStoreId}/transactions`);
                if (!response.ok) {
                    throw new Error(`Failed to load transactions: ${response.statusText}`);
                }
                
                const result = await response.json();
                console.log('Transactions response:', result);
                
                this.transactions = result.data || [];
            } catch (error) {
                console.error('Error loading transactions:', error);
                this.error = error.message;
                this.transactions = [];
            } finally {
                this.loading = false;
            }
        },
        
        async loadRedundantBefore() {
            if (!this.selectedHostId) return;
            
            try {
                const response = await fetch(`/hosts/${this.selectedHostId}/redundant_before`);
                if (!response.ok) {
                    throw new Error(`Failed to load redundant before data: ${response.statusText}`);
                }
                
                const result = await response.json();
                console.log('Redundant before response:', result);
                
                // Transform the flat response into range-grouped format
                const data = result.data || {};

                data.map((v, idx) => {
                    return v.properties = [
                        { property: 'GC_BEFORE', timestamp: v.gcBefore || '[0,0,0(KR),0]' },
                        { property: 'SHARD_APPLIED', timestamp: v.shardApplied || '[0,0,0(KR),0]' },
                        { property: 'QUORUM_APPLIED', timestamp: v.quorumApplied || '[0,0,0(KR),0]' },
                        { property: 'LOCALLY_APPLIED', timestamp: v.locallyApplied || '[0,0,0(KR),0]' },
                        { property: 'LOCALLY_DURABLE_TO_COMMAND_STORE', timestamp: v.locallyDurableToCommandStore || '[0,0,0(KR),0]' },
                        { property: 'LOCALLY_DURABLE_TO_DATA_STORE', timestamp: v.locallyDurableToDataStore || '[0,0,0(KR),0]' },
                        { property: 'LOCALLY_REDUNDANT', timestamp: v.locallyRedundant || '[1,0,0(KR),0]' },
                        { property: 'LOCALLY_SYNCED', timestamp: v.locallySynced || '[0,0,0(KR),0]' },
                        { property: 'LOCALLY_WITNESSED', timestamp: v.locallyWitnessed || '[0,0,0(KR),0]' },
                        { property: 'PRE_BOOTSTRAP', timestamp: v.preBootstrap || '[1,0,0(KR),0]' }
                    ]
                });

                this.redundantBeforeData = data;

            } catch (error) {
                console.error('Error loading redundant before data:', error);
                // Don't set error state for redundant before, as it's supplementary data
                this.redundantBeforeData = [];
            }
        },
        
        async filterByProperty(property) {
            if (this.selectedProperty === property) {
                // Toggle off if same property clicked
                this.clearPropertyFilter();
                return;
            }
            
            this.selectedProperty = property;
            this.loading = true;
            this.error = null;
            
            try {
                const response = await fetch(`/hosts/${this.selectedHostId}/stores/${this.selectedStoreId}/transactions?property=${encodeURIComponent(property)}`);
                if (!response.ok) {
                    throw new Error(`Failed to load filtered transactions: ${response.statusText}`);
                }
                
                const result = await response.json();
                console.log('Property filtered transactions response:', result);
                
                // Store the TxnIds that match the property filter
                this.propertyFilteredTxnIds = (result.data || []).map(txn => txn.txnId);
            } catch (error) {
                console.error('Error loading property filtered transactions:', error);
                this.error = error.message;
                this.propertyFilteredTxnIds = [];
            } finally {
                this.loading = false;
            }
        },
        
        clearPropertyFilter() {
            this.selectedProperty = null;
            this.propertyFilteredTxnIds = [];
        },
        
        isFilteredOut(txn) {
            // If no property filter is active, nothing is filtered out
            if (!this.selectedProperty) return false;
            
            // Transaction is filtered out if it's NOT in the property filtered list
            return !this.propertyFilteredTxnIds.includes(txn.txnId);
        },
        
        openTransactionDetail(txnId) {
            if (!txnId || txnId === 'null') return;
            
            const url = `/txn.html?host=${encodeURIComponent(this.selectedHostId)}&txn_id=${encodeURIComponent(txnId)}`;
            window.open(url, '_blank', 'width=1200,height=800,scrollbars=yes,resizable=yes');
        }
    }
}).mount('#app');