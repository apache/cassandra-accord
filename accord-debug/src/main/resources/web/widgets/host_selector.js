// Vue Host Selector Widget for Cassandra Accord Debug Interface
// Usage: <host-selector v-model="selectedHost" @change="handleHostChange"></host-selector>

(function() {
    'use strict';
    
    // Define the Vue component
    const HostSelector = {
        name: 'HostSelector',
        props: {
            modelValue: {
                type: String,
                default: ''
            },
            disabled: {
                type: Boolean,
                default: false
            }
        },
        emits: ['update:modelValue', 'change'],
        template: `
            <div class="host-selector">
                <label for="hostSelect">Select Host:</label>
                <select 
                    id="hostSelect" 
                    :value="modelValue" 
                    @change="handleChange"
                    :disabled="disabled || loading"
                >
                    <option value="">-- Select a host --</option>
                    <option v-for="host in hosts" :key="host.id" :value="host.id">
                        {{ host.id }}
                    </option>
                </select>
                <span v-if="loading" class="loading-indicator">Loading hosts...</span>
                <span v-if="error" class="error-indicator">{{ error }}</span>
            </div>
        `,
        data() {
            return {
                hosts: [],
                loading: false,
                error: null
            };
        },
        mounted() {
            this.loadAvailableHosts();
        },
        methods: {
            async loadAvailableHosts() {
                this.loading = true;
                this.error = null;
                
                try {
                    const response = await fetch('/hosts');
                    if (response.ok) {
                        const result = await response.json();
                        this.hosts = result.data || [];
                        console.log('Host selector loaded hosts:', this.hosts);
                    } else {
                        throw new Error(`Failed to load hosts: ${response.statusText}`);
                    }
                } catch (error) {
                    console.error('Failed to load hosts:', error);
                    this.error = 'Failed to load hosts';
                    this.hosts = [];
                } finally {
                    this.loading = false;
                }
            },
            
            handleChange(event) {
                const value = event.target.value;
                this.$emit('update:modelValue', value);
                this.$emit('change', value);
            }
        },
        styles: `
            .host-selector {
                margin-bottom: 2rem;
                padding: 1rem;
                background: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            
            .host-selector label {
                font-weight: 500;
                color: #495057;
                margin-right: 0.5rem;
            }
            
            .host-selector select {
                padding: 0.5rem;
                border: 1px solid #ddd;
                border-radius: 4px;
                margin-left: 0.5rem;
                min-width: 200px;
            }
            
            .host-selector select:disabled {
                background-color: #f8f9fa;
                color: #6c757d;
            }
            
            .loading-indicator {
                margin-left: 0.5rem;
                color: #0c5460;
                font-size: 0.9em;
                font-style: italic;
            }
            
            .error-indicator {
                margin-left: 0.5rem;
                color: #721c24;
                font-size: 0.9em;
                font-weight: 500;
            }
        `
    };
    
    // Inject styles into the document
    if (!document.getElementById('host-selector-styles')) {
        const style = document.createElement('style');
        style.id = 'host-selector-styles';
        style.textContent = HostSelector.styles;
        document.head.appendChild(style);
    }
    
    // Export the component for global use
    window.HostSelectorComponent = HostSelector;
})();