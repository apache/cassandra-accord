// Vue Transaction Widget for Cassandra Accord Debug Interface
// Usage: <transaction-widget :txn-id="someTxnId" type="quorum"></transaction-widget>

(function() {
    'use strict';
    
    // Define the Vue component
    const TransactionWidget = {
        name: 'TransactionWidget',
        props: {
            txnId: {
                type: String,
                required: true
            },
            type: {
                type: String,
                default: 'default'
            }
        },
        template: `
            <div class="transaction-widget" :class="'transaction-widget--' + type">
                <div class="txn-id timestamp clickable" 
                     :class="getTimestampClass(txnId)"
                     :title="formatTimestampWithRecency(txnId)"
                     @click="openTransactionSearch(txnId)">
                    {{ txnId }}
                </div>
            </div>
        `,
        methods: {
            openTransactionSearch(txnId) {
                const url = `/txn.html?txn_id=${encodeURIComponent(txnId)}`;
                window.open(url, '_blank', 'width=1200,height=800,scrollbars=yes,resizable=yes');
            },
            
            formatTimestamp(timestampStr) {
                if (!timestampStr || timestampStr === 'N/A') {
                    return 'No timestamp available';
                }
                
                try {
                    // Parse timestamp format: [18,1756144112990011,130(KW),1]
                    const match = timestampStr.match(/\[(\d+),(\d+),(\d+).*\]/);
                    if (!match) {
                        return 'Invalid timestamp format';
                    }
                    
                    const microTimestamp = parseInt(match[2]);
                    const millisTimestamp = Math.floor(microTimestamp / 1000);
                    const date = new Date(millisTimestamp);
                    
                    if (isNaN(date.getTime())) {
                        return 'Invalid timestamp';
                    }
                    
                    // Format as DD-MM-YYYY HH-MM-SS.mmm
                    const day = String(date.getDate()).padStart(2, '0');
                    const month = String(date.getMonth() + 1).padStart(2, '0');
                    const year = date.getFullYear();
                    const hours = String(date.getHours()).padStart(2, '0');
                    const minutes = String(date.getMinutes()).padStart(2, '0');
                    const seconds = String(date.getSeconds()).padStart(2, '0');
                    const milliseconds = String(date.getMilliseconds()).padStart(3, '0');
                    
                    return `${day}-${month}-${year} ${hours}-${minutes}-${seconds}.${milliseconds}`;
                } catch (error) {
                    console.error('Error parsing timestamp:', timestampStr, error);
                    return 'Error parsing timestamp';
                }
            },
            
            formatTimestampWithRecency(timestampStr) {
                const formattedDate = this.formatTimestamp(timestampStr);
                if (formattedDate === 'No timestamp available' || formattedDate === 'Invalid timestamp format' || formattedDate === 'Error parsing timestamp') {
                    return formattedDate;
                }
                
                const recency = this.getTimestampRecency(timestampStr);
                if (recency) {
                    return `${formattedDate} (${recency})`;
                }
                return formattedDate;
            },
            
            getTimestampRecency(timestampStr) {
                if (!timestampStr || timestampStr === 'N/A') {
                    return null;
                }
                
                try {
                    const match = timestampStr.match(/\[(\d+),(\d+),(\d+).*\]/);
                    if (!match) {
                        return null;
                    }
                    
                    const microTimestamp = parseInt(match[2]);
                    const millisTimestamp = Math.floor(microTimestamp / 1000);
                    const timestampDate = new Date(millisTimestamp);
                    const now = new Date();
                    
                    if (isNaN(timestampDate.getTime())) {
                        return null;
                    }
                    
                    const diffMinutes = Math.floor((now - timestampDate) / (1000 * 60));
                    
                    if (diffMinutes < 1) {
                        return 'just now';
                    } else if (diffMinutes === 1) {
                        return '1 minute ago';
                    } else if (diffMinutes < 60) {
                        return `${diffMinutes} minutes ago`;
                    } else if (diffMinutes < 120) {
                        return '1 hour ago';
                    } else if (diffMinutes < 1440) {
                        const hours = Math.floor(diffMinutes / 60);
                        return `${hours} hours ago`;
                    } else {
                        const days = Math.floor(diffMinutes / 1440);
                        return days === 1 ? '1 day ago' : `${days} days ago`;
                    }
                } catch (error) {
                    return null;
                }
            },
            
            getTimestampClass(timestampStr) {
                if (!timestampStr || timestampStr === 'N/A') {
                    return '';
                }
                
                try {
                    const match = timestampStr.match(/\[(\d+),(\d+),(\d+).*\]/);
                    if (!match) {
                        return '';
                    }
                    
                    const microTimestamp = parseInt(match[2]);
                    const millisTimestamp = Math.floor(microTimestamp / 1000);
                    const timestampDate = new Date(millisTimestamp);
                    const now = new Date();
                    
                    if (isNaN(timestampDate.getTime())) {
                        return '';
                    }
                    
                    const diffMinutes = Math.floor((now - timestampDate) / (1000 * 60));
                    
                    if (diffMinutes <= 10) {
                        return 'recent';  // Green-ish background
                    } else if (diffMinutes <= 20) {
                        return 'moderate';  // Yellow background
                    } else {
                        return 'old';  // Red-ish background
                    }
                } catch (error) {
                    return '';
                }
            }
        }
    };

    // Make component available globally
    if (typeof window !== 'undefined') {
        window.TransactionWidgetComponent = TransactionWidget;
    }

    // Add CSS styles
    const style = document.createElement('style');
    style.textContent = `
        .transaction-widget {
            display: inline-block;
        }
        
        .transaction-widget .txn-id {
            font-family: monospace;
            color: #007bff;
            font-weight: 500;
            cursor: pointer;
            text-decoration: underline dotted;
            transition: color 0.2s;
            padding: 0.25rem 0.5rem;
            border-radius: 3px;
        }
        
        .transaction-widget .txn-id:hover {
            color: #0056b3;
            text-decoration: underline solid;
        }
        
        .transaction-widget .txn-id.recent {
            background-color: #d4edda;
            color: #155724;
        }
        
        .transaction-widget .txn-id.moderate {
            background-color: #fff3cd;
            color: #856404;
        }
        
        .transaction-widget .txn-id.old {
            background-color: #f8d7da;
            color: #721c24;
        }
        
        .transaction-widget--quorum .txn-id {
            border-left: 3px solid #fd7e14;
        }
        
        .transaction-widget--universal .txn-id {
            border-left: 3px solid #6f42c1;
        }
    `;

    if (document.head) {
        document.head.appendChild(style);
    }
})();