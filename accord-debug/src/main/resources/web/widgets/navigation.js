// Vue Navigation Widget for Cassandra Accord Debug Interface
// Usage: <navigation-widget></navigation-widget>

(function() {
    'use strict';
    
    // Define the Vue component
    const NavigationWidget = {
        name: 'NavigationWidget',
        template: `
            <div class="navigation-widget">
                <nav class="nav-menu">
                    <a href="/command_store.html" class="nav-link" title="View command store data">
                        Command Store
                    </a>
                    <a href="/commands_for_key.html" class="nav-link" title="View commands for specific routing keys">
                        Commands For Key
                    </a>
                    <a href="/coordinations.html" class="nav-link" title="View coordination activities">
                        Coordinations
                    </a>
                    <a href="/durability_service.html" class="nav-link" title="View durability service status">
                        Durability Service
                    </a>
                    <a href="/durable_before.html" class="nav-link" title="View durable before operations">
                        Durable Before
                    </a>
                    <a href="/progress_log.html" class="nav-link" title="View progress log">
                        Progress Log
                    </a>
                    <a href="/redundant_before.html" class="nav-link" title="View redundant before operations">
                        Redundant Before
                    </a>
                    <a href="/topologies.html" class="nav-link" title="View cluster topologies">
                        Topologies
                    </a>
                </nav>
            </div>
        `
    };

    // Make component available globally
    if (typeof window !== 'undefined') {
        window.NavigationWidgetComponent = NavigationWidget;
    }

    // Add CSS styles
    const style = document.createElement('style');
    style.textContent = `
        .navigation-widget {
            margin: 1rem 0;
            padding: 1rem;
            background-color: #f8f9fa;
            border-radius: 5px;
            border: 1px solid #dee2e6;
        }
        
        .nav-menu {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            align-items: center;
        }
        
        .nav-link {
            display: inline-block;
            padding: 0.5rem 1rem;
            color: #007bff;
            text-decoration: none;
            background-color: white;
            border: 1px solid #007bff;
            border-radius: 4px;
            font-weight: 500;
            transition: all 0.2s ease;
        }
        
        .nav-link:hover {
            background-color: #007bff;
            color: white;
            text-decoration: none;
            transform: translateY(-1px);
            box-shadow: 0 2px 4px rgba(0, 123, 255, 0.2);
        }
        
        .nav-link:active {
            transform: translateY(0);
            box-shadow: 0 1px 2px rgba(0, 123, 255, 0.2);
        }
        
        @media (max-width: 768px) {
            .nav-menu {
                flex-direction: column;
                align-items: stretch;
            }
            
            .nav-link {
                text-align: center;
            }
        }
    `;

    if (document.head) {
        document.head.appendChild(style);
    }
})();