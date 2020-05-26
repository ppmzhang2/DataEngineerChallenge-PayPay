package com.paypay.quiz.models

import java.sql.Timestamp

case class LogFmt(
                timestamp: Timestamp,
                elb: String,
                client_ip: String,
                client_port: Int,
                backend_ip: String,
                backend_port: Int,
                request_processing_time: Double,
                backend_processing_time: Double,
                response_processing_time: Double,
                elb_status_code: String,
                backend_status_code: String,
                received_bytes: Long,
                sent_bytes: Long,
                request_action: String,
                request_url: String,
                request_protocol: String,
                user_agent: String,
                ssl_cipher: String,
                ssl_protocol: String
              )
