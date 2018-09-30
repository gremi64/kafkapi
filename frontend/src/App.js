import React, { Component } from 'react';
import { Container, Menu } from 'semantic-ui-react';

import OffsetsPanel from './components/offsets/OffsetsPanel';
import MessagesPanel from './components/messages/MessagesPanel';

class App extends Component {

    constructor(props) {
        super(props);
        this.state = {
            activeItem: 'Messages',
        };
    };

    handleMenuClick = (e, { name }) => this.setState({ activeItem: name });

    render() {
        return (
            <div className="App">
                <Container>
                    <Menu pointing secondary>
                        <Menu.Item name='Offsets' active={ this.state.activeItem === 'Offsets' } onClick={ this.handleMenuClick } />
                        <Menu.Item name='Messages' active={ this.state.activeItem === 'Messages' } onClick={ this.handleMenuClick } />
                    </Menu> 
                </Container>

                { this.state.activeItem === 'Offsets' && <OffsetsPanel /> }

                { this.state.activeItem === 'Messages' && <MessagesPanel /> }
            </div>
        );
    };
}

export default App;
