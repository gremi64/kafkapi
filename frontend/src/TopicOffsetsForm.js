import React, { Component } from 'react';

import { Button, Container, Form } from 'semantic-ui-react'

class TopicOffsetsForm extends Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
            <Container>
                <Form>    
                    <Form.Group widths='equal'>
                    <Form.Input
                        fluid
                        id='form-subcomponent-shorthand-input-first-name'
                        label='Topic'
                        placeholder='Topic'
                    />
                    <Form.Input
                        fluid
                        id='form-subcomponent-shorthand-input-last-name'
                        label='Group'
                        placeholder='Group'
                    />
                </Form.Group>
                    <Button type='submit'>{this.props.buttonTitle}</Button>
                </Form>
            </Container>
        );
    }
}

export default TopicOffsetsForm;
