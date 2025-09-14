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
    data() {
        return {
            messageInput: '',
            messages: [],
            filteredMessages: [],
            processedMessages: [],
            processes: [],
            selectedMessage: null,
            highlightedMessages: [],
            error: null,

            isDragOver: false,
            isDragActive: false,
            
            // Filter state
            messageTypes: [],  // Array of {name, count, enabled}
            messageTypeFilters: new Map(),  // Map of message_kind -> boolean
            tags: [],  // Array of {name, count, enabled}
            tagFilters: new Map(),  // Map of tag -> boolean

            // Paging
            activePage: 0,
            pages: [],
            messagesPerPage: 100,

            // Layout constants
            diagramWidth: 1200,
            diagramHeight: 800,
            headerHeight: 50,
            footerHeight: 30,
            processSpacing: 500,
            timeStep: 50,
            selfLoopWidth: 100
        };
    },
    
    mounted() {
        // Load sample data on start
        this.loadSampleData();
        
        // Add keyboard event listener
        document.addEventListener('keydown', this.handleKeyDown);
    },
    
    beforeUnmount() {
        document.removeEventListener('keydown', this.handleKeyDown);
    },
    
    methods: {
        formatDateTime(timestamp) {
            if (!timestamp) return 'N/A';
            
            try {
                const date = new Date(timestamp);
                const day = String(date.getDate()).padStart(2, '0');
                const month = String(date.getMonth() + 1).padStart(2, '0');
                const year = String(date.getFullYear()).slice(-2);
                const hours = String(date.getHours()).padStart(2, '0');
                const minutes = String(date.getMinutes()).padStart(2, '0');
                const seconds = String(date.getSeconds()).padStart(2, '0');
                const milliseconds = String(date.getMilliseconds()).padStart(3, '0');
                
                return `${day}/${month}/${year} ${hours}:${minutes}:${seconds}:${milliseconds}`;
            } catch (error) {
                return timestamp; // Return original if parsing fails
            }
        },
        
        truncateText(text, maxLength = 20) {
            if (!text) return '';
            return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
        },
        
        loadSampleData() {
            const sampleData = [];
            
            this.messageInput = JSON.stringify(sampleData, null, 2);
            this.loadMessages();
        },
        
        loadMessages() {
            this.error = null;
            this.selectedMessage = null;
            this.highlightedMessages = [];
            
            try {
                this.messages = JSON.parse(this.messageInput);
                
                if (!Array.isArray(this.messages)) {
                    throw new Error('Input must be an array of messages');
                }
                
                this.processMessages();
            } catch (e) {
                this.error = `Error parsing messages: ${e.message}`;
                this.messages = [];
                this.filteredMessages = [];
                this.processedMessages = [];
                this.processes = [];
            }
        },

        // TODO: avoid looping over the messages multiple times
        processMessages() {
            // Extract message types and tags and initialize filters
            this.extractMessageTypes();
            this.extractTags();

            const seenSent = new Set();
            const seenReceived = new Set();
            // Filter messages based on current filter state
            this.filteredMessages = this.messages.filter(msg => {
                // Check message type filter
                if (this.messageTypeFilters.get(msg.message_kind) === false) {
                    return false;
                }

                // Check tag filters - message must have at least one enabled tag, or no tags required if all tags enabled
                if (this.tags.length > 0) {
                    if (msg.tags && msg.tags.length > 0) {
                        const hasEnabledTag = msg.tags.some(tag => this.tagFilters.get(tag) !== false);
                        if (!hasEnabledTag) {
                            return false;
                        }
                    }
                    else
                        // If tag filtering is enabled, filter out messages with no tags
                        return false;
                }

                // while (seenSent.has(msg.sent_at)) msg.sent_at++;
                // while (seenReceived.has(msg.received_at)) msg.received_at++;
                seenSent.add(msg.sent_at);
                seenReceived.add(msg.received_at);
                return true;
            });

            const pagesTotal = Math.ceil(this.filteredMessages.length / this.messagesPerPage);

            this.pages = [];
            for (let i = 0; i < pagesTotal; i++)
                this.pages.push(i);

            // Extract all processes from filtered messages
            const processSet = new Set();
            this.filteredMessages.forEach(msg => {
                processSet.add(msg.from);
                processSet.add(msg.to);
            });
            this.processes = Array.from(processSet).sort();

            let filteredMessages = this.filteredMessages.slice(this.activePage * this.messagesPerPage, this.activePage * this.messagesPerPage + this.messagesPerPage);

            // Create timestamp mapping for normalization from filtered messages
            const allTimestamps = new Set();
            const reqReceivedAt = new Map();
            filteredMessages.forEach(msg => {
                if (msg && msg.message_kind.endsWith("_REQ"))
                    reqReceivedAt.set([msg.to, msg.id], msg.received_at);
                allTimestamps.add(msg.sent_at);
                allTimestamps.add(msg.received_at);
            });
            
            const sortedTimestamps = Array.from(new Set(allTimestamps)).sort((a, b) => a - b);
            const timeMap = new Map();
            sortedTimestamps.forEach((timestamp, index) => {
                timeMap.set(timestamp, index);
            });
            
            // Process filtered messages with normalized timestamps
            this.processedMessages = filteredMessages.map(msg => {
                var sent_at = msg.sent_at;
                var received_at = msg.sent_at;
                if (msg && msg.message_kind.endsWith("_RSP") && reqReceivedAt.has([msg.from, msg.id]))
                {
                    sent_at = reqReceivedAt.get([msg.from, msg.id]);
                    received_at = sent_at + 1;
                }
                return {
                    ...msg,
                    normalizedSentAt: timeMap.get(sent_at),
                    normalizedReceivedAt: timeMap.get(received_at) + 1
                };
            })
            
            // Calculate diagram dimensions
            this.calculateDimensions(timeMap);
        },

        // TODO: message type counts are unused; do we want to use them?
        extractMessageTypes() {
            // Count all message types from all messages (not filtered)
            const typeCount = new Map();
            this.messages.forEach(msg => {
                const kind = msg.message_kind;
                typeCount.set(kind, (typeCount.get(kind) || 0) + 1);
            });
            
            // Create or update message types array
            const existingFilters = new Map(this.messageTypeFilters);
            this.messageTypes = Array.from(typeCount.entries())
                .map(([name, count]) => ({
                    name,
                    count,
                    enabled: existingFilters.has(name) ? existingFilters.get(name) : true
                }))
                .sort((a, b) => a.name.localeCompare(b.name));
            
            // Update filter state
            this.messageTypeFilters.clear();
            this.messageTypes.forEach(type => {
                this.messageTypeFilters.set(type.name, type.enabled);
            });
        },

        // TODO: tag counts are unused; do we want to use them?
        extractTags() {
            // Count all tags from all messages (not filtered)
            const tagCount = new Map();
            this.messages.forEach(msg => {
                if (msg.tags && Array.isArray(msg.tags)) {
                    msg.tags.forEach(tag => {
                        tagCount.set(tag, (tagCount.get(tag) || 0) + 1);
                    });
                }
            });
            
            // Create or update tags array
            const existingTagFilters = new Map(this.tagFilters);
            this.tags = Array.from(tagCount.entries())
                .map(([name, count]) => ({
                    name,
                    count,
                    enabled: existingTagFilters.has(name) ? existingTagFilters.get(name) : true
                }))
                .sort((a, b) => a.name.localeCompare(b.name));
            
            // Update tag filter state
            this.tagFilters.clear();
            this.tags.forEach(tag => {
                this.tagFilters.set(tag.name, tag.enabled);
            });
        },

        /**
         * Rendering
         */

        calculateDimensions(timeMap) {
            const maxProcesses = this.processes.length;
            const maxTime = timeMap.size > 0 ? Math.max(...Array.from(timeMap.values())) : 0;
            
            // Ensure minimum width for readability and add padding for process labels
            this.diagramWidth = Math.max(1200, maxProcesses * this.processSpacing + 400);
            
            // Ensure adequate height for all time steps with extra padding
            this.diagramHeight = Math.max(600, maxTime * this.timeStep + this.headerHeight + this.footerHeight + 200);
        },
        
        getProcessX(processName) {
            const index = this.processes.indexOf(processName);
            return 100 + index * this.processSpacing;
        },
        
        getMessageY(normalizedTime) {
            return this.headerHeight + 50 + normalizedTime * this.timeStep;
        },
        
        getLabelX(message) {
            if (message.from === message.to) {
                return this.getProcessX(message.from) + this.selfLoopWidth / 2;
            }
            const fromX = this.getProcessX(message.from);
            const toX = this.getProcessX(message.to);
            return (fromX + toX) / 2;
        },
        
        getLabelY(message) {
            if (message.from === message.to) {
                return this.getMessageY(message.normalizedSentAt) - 15;
            }
            const fromY = this.getMessageY(message.normalizedSentAt);
            const toY = this.getMessageY(message.normalizedReceivedAt);
            return (fromY + toY) / 2 - 10;
        },
        
        getLabelWidth(text) {
            // Approximate text width calculation
            return text.length * 6;
        },
        
        getArrowPoints(message) {
            const fromX = this.getProcessX(message.from);
            const fromY = this.getMessageY(message.normalizedSentAt);
            const toX = this.getProcessX(message.to);
            const toY = this.getMessageY(message.normalizedReceivedAt);
            
            // Calculate arrow direction
            const dx = toX - fromX;
            const dy = toY - fromY;
            const length = Math.sqrt(dx * dx + dy * dy);
            
            if (length === 0) return '';
            
            const unitX = dx / length;
            const unitY = dy / length;
            
            // Arrow head size
            const arrowSize = 8;
            
            // Arrow head points
            const tipX = toX - 4 * unitX; // Offset from circle
            const tipY = toY - 4 * unitY;
            
            const leftX = tipX - arrowSize * unitX - 4 * unitY;
            const leftY = tipY - arrowSize * unitY + 4 * unitX;
            
            const rightX = tipX - arrowSize * unitX + 4 * unitY;
            const rightY = tipY - arrowSize * unitY - 4 * unitX;
            
            return `${tipX},${tipY} ${leftX},${leftY} ${rightX},${rightY}`;
        },
        
        getSelfMessagePath(message) {
            const x = this.getProcessX(message.from);
            const y = this.getMessageY(message.normalizedSentAt);
            const width = this.selfLoopWidth;
            const height = 20;
            
            // Create oval path for self-message
            return `M ${x + 4} ${y} Q ${x + width} ${y - height} ${x + 4} ${y - 2}`;
        },
        
        getSelfMessageArrowPoints(message) {
            const x = this.getProcessX(message.from);
            const y = this.getMessageY(message.normalizedSentAt);
            
            // Arrow pointing back to the circle
            return `${x + 4},${y - 2} ${x + 12},${y - 6} ${x + 12},${y + 2}`;
        },
        
        onMessageClick(message) {
            if (message === this.selectedMessage)
            {
                this.selectedMessage = null;
                this.highlightedMessages = [];
                return;
            }
            this.selectedMessage = message;
            this.highlightRelatedMessages(message);
        },

        // TODO (required): implement highlights by checking if there's a match in tags
        highlightRelatedMessages(message) {
            if (!message.tags || message.tags.length === 0) {
                this.highlightedMessages = [message];
                return;
            }
            
            this.highlightedMessages = this.processedMessages.filter(msg => {
                if (msg === message) return true;
                if (!msg.tags || msg.tags.length === 0) return false;
                
                // Check if any tags match
                return msg.tags.some(tag => message.tags.includes(tag));
            });
        },
        
        isMessageHighlighted(message) {
            return this.highlightedMessages.includes(message);
        },
        
        isProcessHighlighted(processName) {
            return this.highlightedMessages.some(msg => 
                msg.from === processName || msg.to === processName
            );
        },
        
        handleKeyDown(event) {
            if (event.key === 'Escape') {
                this.selectedMessage = null;
                this.highlightedMessages = [];
            }
        },

        /**
         * File upload handling
         **/

        // Drag and drop event handlers
        handleDragEnter(event) {
            event.preventDefault();
            this.isDragActive = true;
        },
        
        handleDragOver(event) {
            event.preventDefault();
            this.isDragOver = true;
        },
        
        handleDragLeave(event) {
            event.preventDefault();
            // Only reset drag state if leaving the drag zone completely
            if (!event.currentTarget.contains(event.relatedTarget)) {
                this.isDragOver = false;
                this.isDragActive = false;
            }
        },
        
        handleDrop(event) {
            event.preventDefault();
            this.isDragOver = false;
            this.isDragActive = false;
            
            const files = event.dataTransfer.files;
            if (files.length > 0) {
                const file = files[0];
                
                // Check if it's a JSON file
                if (file.type === 'application/json' || file.name.endsWith('.json')) {
                    this.readJsonFile(file);
                } else {
                    this.error = `Invalid file type: ${file.type || 'unknown'}. Please upload a JSON file.`;
                }
            }
        },
        
        readJsonFile(file) {
            this.error = null;
            
            const reader = new FileReader();
            
            reader.onload = (event) => {
                try {
                    const content = event.target.result;
                    this.messageInput = content;
                    
                    // Automatically load the messages from the file
                    this.messages = JSON.parse(content);
                    
                    if (!Array.isArray(this.messages)) {
                        throw new Error('File must contain an array of messages');
                    }
                    
                    this.processMessages();
                    
                } catch (e) {
                    this.error = `Error reading file: ${e.message}`;
                    this.messages = [];
                    this.processedMessages = [];
                    this.processes = [];
                }
            };
            
            reader.onerror = () => {
                this.error = 'Error reading file. Please try again.';
            };
            
            reader.readAsText(file);
        },


        /**
         * Message types
         */

        toggleMessageType(messageKind) {
            const currentState = this.messageTypeFilters.get(messageKind);
            this.messageTypeFilters.set(messageKind, !currentState);
            
            // Update messageTypes array
            const typeIndex = this.messageTypes.findIndex(t => t.name === messageKind);
            if (typeIndex >= 0) {
                this.messageTypes[typeIndex].enabled = !currentState;
            }
            
            // Reprocess messages with new filter
            this.processMessages();
        },
        
        enableAllMessageTypes() {
            this.messageTypes.forEach(type => {
                type.enabled = true;
                this.messageTypeFilters.set(type.name, true);
            });
            this.processMessages();
        },
        
        disableAllMessageTypes() {
            this.messageTypes.forEach(type => {
                type.enabled = false;
                this.messageTypeFilters.set(type.name, false);
            });
            this.processMessages();
        },

        /**
         * Tags
         */

        toggleTag(tagName) {
            const currentState = this.tagFilters.get(tagName);
            this.tagFilters.set(tagName, !currentState);
            
            // Update tags array
            const tagIndex = this.tags.findIndex(t => t.name === tagName);
            if (tagIndex >= 0) {
                this.tags[tagIndex].enabled = !currentState;
            }
            
            // Reprocess messages with new filter
            this.processMessages();
        },

        enableAllTags() {
            this.tags.forEach(tag => {
                tag.enabled = true;
                this.tagFilters.set(tag.name, true);
            });
            this.processMessages();
        },
        
        disableAllTags() {
            this.tags.forEach(tag => {
                tag.enabled = false;
                this.tagFilters.set(tag.name, false);
            });
            this.processMessages();
        },

        /**
         * Paging
         */
        togglePage(page) {
            this.activePage = page;
            this.processMessages();
        }
    }
}).mount('#app');