pub mod google {
    pub mod spanner {
        pub mod v1 {
            tonic::include_proto!("google.spanner.v1");
        }
    }
    #[allow(dead_code)]
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}
