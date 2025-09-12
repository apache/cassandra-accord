// Vue Routing Key Widget for Cassandra Accord Debug Interface
// Usage: <routing-key-widget :keys="keyArray" :host-id="hostId"></routing-key-widget>

(function() {
    'use strict';
    
    // Define the Vue component
    const RoutingKeyWidget = {
        name: 'RoutingKeyWidget',
        props: {
            keys: {
                type: Array,
                default: () => []
            },
            hostId: {
                type: String,
                default: ''
            },
            showClickHint: {
                type: Boolean,
                default: true
            }
        },
        template: `
            <div class="routing-key-widget">
                <span v-if="keys.length === 0" class="no-keys">No keys</span>
                <span v-else
                      v-for="key in keys"
                      :key="key"
                      class="routing-key-item clickable"
                      @click="handleKeyClick(key)"
                      :title="showClickHint ? 'Click to view CommandsForKey for: ' + key : key">
                    {{ key || 'null' }}
                </span>
            </div>
        `,
        methods: {
            handleKeyClick(key) {
                if (!key || key === 'null') return;
                
                if (!this.hostId) {
                    console.warn('RoutingKeyWidget: No hostId provided, cannot open CommandsForKey');
                    return;
                }
                
                // Open the commands_for_key.html page in a new tab with the routing key and host
                const url = `/commands_for_key.html?host=${encodeURIComponent(this.hostId)}&key=${encodeURIComponent(key)}`;
                window.open(url, '_blank', 'width=1200,height=800,scrollbars=yes,resizable=yes');
            }
        },
        styles: `
            .routing-key-widget {
                display: inline-flex;
                flex-wrap: wrap;
                gap: 0.5rem;
                align-items: center;
            }
            
            .routing-key-item {
                background: #e9ecef;
                padding: 0.25rem 0.5rem;
                border-radius: 3px;
                font-family: monospace;
                font-size: 0.85em;
                border: 1px solid #dee2e6;
                transition: all 0.2s;
                cursor: pointer;
                display: inline-block;
            }
            
            .routing-key-item:hover {
                background: #007bff;
                color: white;
                border-color: #007bff;
                transform: translateY(-1px);
                box-shadow: 0 2px 4px rgba(0,123,255,0.3);
            }
            
            .routing-key-item.clickable {
                cursor: pointer;
            }
            
            .no-keys {
                color: #6c757d;
                font-style: italic;
                font-size: 0.9em;
            }
            
            /* Compact variant for smaller spaces */
            .routing-key-widget.compact .routing-key-item {
                padding: 0.15rem 0.4rem;
                font-size: 0.8em;
            }
            
            /* List variant for vertical layout */
            .routing-key-widget.list {
                flex-direction: column;
                align-items: flex-start;
            }
            
            .routing-key-widget.list .routing-key-item {
                margin-bottom: 0.25rem;
                width: 100%;
                text-align: left;
            }
        `
    };
    
    // Inject styles into the document
    if (!document.getElementById('routing-key-widget-styles')) {
        const style = document.createElement('style');
        style.id = 'routing-key-widget-styles';
        style.textContent = RoutingKeyWidget.styles;
        document.head.appendChild(style);
    }
    
    // Export the component for global use
    window.RoutingKeyWidgetComponent = RoutingKeyWidget;
})();