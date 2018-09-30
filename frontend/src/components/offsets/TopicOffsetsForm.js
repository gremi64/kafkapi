import React, { Component } from 'react';

import { Button, Container, Form } from 'semantic-ui-react'

class TopicOffsetsForm extends Component {
    constructor(props) {
        super(props);
        this.state = {
            topicForm: props.topicForm,
            groupForm: props.groupForm,
            brokers: props.brokers,
            brokersForm: "",
            securities: props.securities,
            securityForm: "",
        };

        //this.handleBrokersChange = this.handleBrokersChange.bind(this);
    }
    
    handleBrokersChange = (event, data) => {
        const { value } = data;
        const { key } = data.options.find(o => o.value === value);
        console.log('Selected brokers key > ' + key);
        
        this.setState({brokersForm: key});
    }

    handleSecurityChange = (event, data) => {
        const { value } = data;
        const { key } = data.options.find(o => o.value === value);
        console.log('Selected security key > ' + key);
        
        this.setState({securityForm: key});
    }

    onClickForm = () => {
        this.props.onClickForm(this.state.topicForm, this.state.groupForm, this.state.brokersForm, this.state.securityForm);
    }

    render() {
        return (
            <Container>
                <Form>
                    <Form.Group widths='equal'>
                        <Form.Input
                            fluid
                            id='myTopic'
                            label='Topic'
                            value={ this.state.topicForm }
                            onChange={e => this.setState({
                                topicForm: e.target.value
                            })}
                        />
                        <Form.Input
                            fluid
                            id='myGroup'
                            label='Group'
                            value={ this.state.groupForm }
                            onChange={e => this.setState({
                                groupForm: e.target.value
                            })}
                        />
                    </Form.Group>
                    <Form.Group widths='equal'>
                        <Form.Dropdown  fluid placeholder='Select brokers' search selection options={ this.props.brokers } onChange={ this.handleBrokersChange } />
                        <Form.Dropdown fluid placeholder='Select security' search selection options={ this.props.securities } onChange={ this.handleSecurityChange } />
                    </Form.Group>
                    <Button onClick={ this.onClickForm }>Let's find my offsets</Button>
                </Form>
                
            </Container>
        );
    }
}

export default TopicOffsetsForm;
