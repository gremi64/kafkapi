import React, { Component } from 'react';
import OffsetsPanel from './components/offsets/OffsetsPanel';
import { Container, Menu } from 'semantic-ui-react';

class App extends Component {

    constructor(props) {
        super(props);
        this.state = {
            activeItem: 'Offsets',
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

                { this.state.activeItem === 'Offsets' && <OffsetsPanel />}

                { this.state.activeItem === 'Messages' && <Container>
                    Ici on va afficher des messages
                </Container>}
            </div>
        );
    };
}

export default App;
