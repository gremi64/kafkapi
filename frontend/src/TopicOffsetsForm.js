import React, { Component } from 'react';

import { Button, Container, Form } from 'semantic-ui-react'

class TopicOffsetsForm extends Component {
    constructor(props) {
        super(props);
        this.state = {
            topicForm: props.topicForm,
            groupForm: props.groupForm,
            brokers: props.brokers,
            securities: props.securities
        };
    }

    render() {
        return (
            <Container>
                <Form onSubmit={this.props.handleClick}>
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
                        <Form.Dropdown fluid placeholder='Select brokers' search selection options={ this.props.brokers } />
                        <Form.Dropdown fluid placeholder='Select security' search selection options={ this.props.securities } />
                    </Form.Group>
                    <Button type='submit'>Let's find my offsets</Button>
                </Form>
                
            </Container>
        );
    }
}

export default TopicOffsetsForm;
